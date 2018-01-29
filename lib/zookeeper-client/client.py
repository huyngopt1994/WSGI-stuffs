"""Initialize zookeeper client."""

import logging
import os 
import signal
import sys
import time

from kazoo.client import KazooClient
from kazoo.handlers.gevent import SequentailGeventHandler
from kazoo.protocol.states import KazooState

DEFAULT_CONNECT_TIMEOUT = 60.0

def die_if_suspended_too_long(zk, timeout=10):
    """Long running loop that send `SIGTERM` to process
    if Kazoo has been in state SUSPENDING for more than
    `timeout` seconds.
    """
    suspended = None
    while True:
        if zk.state == KazooState.SUSPENDED:
            # Handle being disconnected from Zookeeper
            if suspended is None:
                suspended = time.time()
            
            if (time.time() - suspended > timeout):
                print("Kazoo in state SUSPENDED for too long, exiting.")
                os.kill(os.getpid(), signal.SIGTERM)
                sys.exit(1)
        else:
            # Handle being connected/reconnected to Zookeeper
            suspended = None
        time.sleep(1)

def die_on_lost_zk_session(state):
    """Default kazoo state handler. Exits on session expired."""
    #get prev_zk_state 
    prev_zk_state = die_on_lost_zk_session.prev_zk_state
    die_on_lost_zk_session.prev_zk_state = state
    if state  == KazooState.LOST and prev_zk_state:
        print("Zookeeper session lost, exiting.")
        os.kill(os.getpid(), signal.SIGTERM)
        return sys.exit(1)

die_on_lost_zk_session.prev_zk_state = None

def get_coordinators():
    return os.environ.get("COORDINATORS") or "127.0.0.1:2181"

def create_client(host=get_coordinators(),
                  state_listener= die_on_lost_zk_session, **kwargs):
    """Create and start a kazoo client."""
    kazoo_logger = logging.getLogger('kazoo.client')
    kazoo_logger.setLevel(logging.WARNING)

    # We use this  SequenceGeventHandler when we want to use gevent
    kwargs.setdefault('handler', SequentailGeventHandler)
    kwargs.setdefault('timeout', DEFAULT_CONNECT_TIMEOUT)
    # Create Kazooclient
    client = KazooClient(host, logger=kazoo_logger, **kwargs)
    client.start()

    # Add handler when state of client was changed
    if state_listener:
        client.add_listener(state_listener)
    
    return client