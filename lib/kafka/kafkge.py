from __future__ import  absolute_import

import json
import logging
import os
import time

import gevent
import kafka
import six

CONSUMER_DEFAULT_POLL_INTERVAL = 2000
DEFAULT_GROUP_ID = kafka.KafkaConsumer.DEFAULT_CONFIG['group_id']

def _get_brokers(zk):
	"""Return a list of  in a brokers with host:port"""
	brokers = []
	for child in zk.get_children('/brokers/ids/'):
		data, _ = zk.get(os.path.join('/brokers/ids', child))
		data = json.loads(data.decode('utf-8'))
		brokers.append('%(host)s:%(port)d' % data)
	return brokers


class AlreadyRunningError(kafka.errors.KafkaError):
	"""The Kafka consumer has already been started."""

class Consumer(object):
	"""A consumer  for a kafka topic. This si non-blocking and will
	 consume message in greenlet

	 :param kazoo.client.KazooClient zk: A Kazoo client.
	 :param str group_id: Name of consumer group to join for dynamically
	    partition assignment( if enabled), and to use for fetching and comitting
	    offsets. If none, auto-patition assignment(via group cooridnatior) and offset commits
	    are disaled.
	    Default: 'kafka-python-default-group'
	 :param callable value_deserializer: Any callable that takes a raw message value and
		returns a deserialized value.
	 :param callable key_deserializer: Any callable that takes a raw message key and returns
	    a deserialized key.
	 :param int poll_interval_ms: Records are fetched and returned in batches by topic
	    -parition. On each poll, consumer will try to use the last consumer offset as starting offset
	    and fetch sequentially. Setting this to a positive integer value will result in the callback
	    receiving multiple recoreds instead of one by one. Default None(off).
	 Some Example: Consumer

	    Process message one by one

	    >>> def callback(topic, msg, prefix, name):
	    ...     print(msg,prefix,name)
	    ...
	    >>> consumer = Consumer(zk,group_id="huy_group")
	    >>> consumer.subscribe('huy_topic')
	    >>> consumer.start(callback, 'hello', name='Peter'))

	    Bulk processing

	    >>> def callback(topic, msgs, prefix, name):
	    ...     for msg in msgs:
	    ...         print(msg, prefix, name)
	    ...
	    >>> consumer = Consumer(zk, group_id="huy_group", poll_interval_ms=3000)
	    >>> consumer.subscribe('huy_topic')
	    >>> consumer.start(callback, 'hello', name='Peter')
	 """

	def __init__(self, zk, group_id=None,
	             value_deserializer=None,
                 key_deserializer=None,
	             poll_interval_ms=None):
		self._zk = zk
		self._log = logging.getLogger('kafka.consumer')
		self.topics= []
		self.running = False
		self.group_id = group_id or DEFAULT_GROUP_ID
		self.value_deserializer = value_deserializer
		self.key_deserializer = key_deserializer
		self.poll_interval_ms = poll_interval_ms


	def subscribe(self, topic):
		"""A topic to subscribe to. Can be called multiple times to subsribe
		to multiple topics.

		NOTE! remember to check the topic in the callback when subscribing to multiple topics.
		"""
		self.topics.append(topic)

	def start(self, callback_func, *args, **kwargs):
		"""Start  the consumer and proxy args/kwargs to the call back.

		This is non-blocking and will spawn a greenlet to process incoming
		messages.
		"""

		if self.running:
			raise AlreadyRunningError
		# get a list of brokers
		brokers = _get_brokers(self._zk)
		consumer = kafka.KafkaConsumer(
			group_id = self.group_id,
			bootstrap_servers = brokers,
			value_deserializer = self.value_deserializer,
			key_deserializer = self.key_deserializer)
		consumer.subscribe(self.topics)
		self.running = True
		# start a greenlet with function is self._handler
		self.greenlet = gevent.spawn(self._handler, consumer, callback_func,
		                             *args, **kwargs)

	def stop(self):
		"""Stop the consumer."""
		self.running = False
		self.greenlet.kill()

	def _handler(self, consumer, callback, *args, **kwargs):
		if self.poll_interval_ms is not None:
			while self.running:
				self._poll(consumer, callback, *args, **kwargs)
				gevent.sleep(0) # make context switch in here
		else:
			self._iterate(consumer, callback, *args, **kwargs)

	def _iterate(self, consumer, callback, *args, **kwargs):
		"""Handle message  one by one(consequently) as soon as they arrive."""

		for msg in consumer:
			try:
				callback(msg.topic, msg.value, *args, **kwargs)
			except Exception:
				log = self._log.getChild('%s.%s' %(msg.topic, self.group_id))
				log.exception('Error while consuming message: %s', msg)
			finally:
				gevent.sleep(0) #context switch

	def __poll(self, consumer,callback, *args, **kwargs):
		"""Batch processing of messages on a timer.
		This is used ful when processing messages one by one isn't suitable, for example
		when doing bulk insert into ElasticSearch.
		"""

		start = time.time()
		for tp, records in six.iteritems(consumer.poll()):
			try:
				callback(tp.topic, [r.value for r in records], *args, **kwargs)
			except Exception:
				log = self._log.getChild('%s.%s'%(tp.topic, self.group_id))
				log.exception('Error while consuming messages: %s', records)
			finally:
				gevent.sleep(0) # context switch

		# Don't sleep if the processing duration is longer than the
		# poll interval
		elapsed = time.time() - start
		if elapsed * 1000 < self.poll_interval_ms:
			gevent.sleep(self.poll_interval_ms/1000 - elapsed)
			