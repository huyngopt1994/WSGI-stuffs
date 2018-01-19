import os

import redis

from werkzeug.wrappers import Response,Request
from werkzeug.routing import Map,Rule
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.wsgi import SharedDataMiddleware
from werkzeug.utils import redirect
from jinja2 import Environment, FileSystemLoader

#Our application class
class Site(object):
	def __init__(self,config):
		# create redis client
		self.redis = redis.Redis(config['redis_host'], config['redis_port'])

	# this function will hanlde request and send it to approciate Handler
	def dispatch_request(self, request):
		return Response('Hello World')

	#function will was called by our application
	def wsgi_app(self, env, start_response):
		#generate request
		request = Request(env)
		response = self.dispatch_request(request)
		return response(env, start_response)

	# entry point , this will was called by wsgi server
	def __call__(self, env, start_response):
		return self.wsgi_app(env,start_response)

# It's a class factory=> It will be used to create new instance
def create_app(redis_host='localhost', redis_port=6379, with_static=True):
	# passing the config for this application
	app = Site({
		'redis_host': redis_host,
		'redis_port': redis_port
	})
	# using static folder
	if with_static:
		app.wsgi_app = SharedDataMiddleware(app.wsgi_app, {
			'/static': os.path.join(os.path.dirname(__file__), 'static')
		})

	return app