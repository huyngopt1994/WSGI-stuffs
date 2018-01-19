import os
import urlparse
import redis

from werkzeug.wrappers import Response,Request
from werkzeug.routing import Map,Rule
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.wsgi import SharedDataMiddleware
from werkzeug.utils import redirect
from jinja2 import Environment, FileSystemLoader

# function to verify the schema of url
def is_valid_url(url):
	parts = urlparse.urlparse(url)
	# verify the scheme of this url

	return parts.scheme in ('http','https')

# function to encode with base 36 => make it shorter
def base36_encode(number):
	assert number >=0 , 'positive interger required'
	if number == 0 :
		return '0'
	base36 = []
	while number != 0 :
		number, remainder =  divmod(number,36)
		base36.append('0123456789abcdefghijklmnopqrstuvwxyz'[remainder])
	return ''.join(reversed(base36))

# Our application class
class Site(object):
	def __init__(self,config):
		# create redis client
		self.redis = redis.Redis(config['redis_host'], config['redis_port'])
		# create path for template
		print(__file__)
		template_path = os.path.join(os.path.dirname(__file__), 'templates')
		print(template_path)
		self.jinja_env = Environment(loader=FileSystemLoader(template_path),
									 autoescape=True)
		self.url_map = Map([
			Rule('/',endpoint='home'),
			Rule('/<short_id>', endpoint='follow_short_link'),
			Rule('/<short_id>/detailed', endpoint='short_link_details')
		])


	# this function will hanlde request and send it to approciate Handler
	def dispatch_request(self, request):
		adapter = self.url_map.bind_to_environ(request.environ)

		try:
			endpoint, values = adapter.match()
			return getattr(self, 'on_' + endpoint)(request, **values)

		except HTTPException, e:
			return e

	def render_template(self, template_name, **context):
		"""
		:param template_name: this is the name of template we should handle
		:param context: this is some thing we should be rendred by jinja_env
		:return: the callback to web server
		"""
		print(template_name)
		t = self.jinja_env.get_template(template_name)
		return Response(t.render(context), mimetype='text/html')

	def insert_url(self, url):
		"""Well we will insert new url"""

		short_id = self.redis.get('reserve-url:'+ url )
		# if we have it before
		if short_id:
			return short_id
		# if not
		url_num  = self.redis.incr('last-url-id')
		#convert to the short_id
		short_id = base36_encode(url_num)
		# save the new url and the id of it
		self.redis.set('url-target:'+ short_id, url)
		self.redis.set('reverse-url:' + url, short_id)
		return short_id


	def on_home(self,request):
		"""Render for home site"""
		error =None
		url = ''
		print(request)
		#If thisi a POST method we should redirect to redirect view
		if request.method == 'POST':
			url = request.form['url']
			print url
			if not is_valid_url(url):
				error= 'Please a valid URL'
			else:
				short_id = self.insert_url(url)
				# redirect to detailed view
				return redirect('/%s/detailed' % short_id)
		return self.render_template('home.html', error=error, url=url)


	def on_follow_short_link(self,request, short_id):
		"""Redirect View."""
		link_target = self.redis.get('url-target:' + short_id)
		if link_target is None:
			raise NotFound()
		# OK we should record that we click one time on this
		self.redis.incr('click-count:'+ short_id)
		# Ok now we have jump into Detailed View
		return redirect(link_target)

	def on_short_link_details(self, request, short_id):
		"""Detailed View."""
		link_target = self.redis.get('url-target:' + short_id)
		if link_target is None:
			raise NotFound()
		click_count = int(self.redis.get('click-count:'+ short_id) or 0)
		return self.render_template('short_link_details.html',
									link_target=link_target,
									short_id=short_id,
									click_count=click_count)

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
