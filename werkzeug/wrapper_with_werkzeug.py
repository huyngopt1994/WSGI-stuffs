
import os 
import redis 
import urlparse 
import socket

from gevent import pywsgi
from werkzeug.wrappers import Response,Request
from werkzeug.routing import Map,Rule
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.wsgi import SharedDataMiddleware
from werkzeug.utils import redirect 
from jinja2 import Enviroment, FileSystemLoader

def tcp_listener(port, address=''):
	if not address:
		address = '0.0.0.0'
	
	try: 
		port = int(port)
	except ValueError :
		print('port should be a number')
		raise 

	"Bind and listen to a TCP socket."
	# create a TCP/IP socket
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	
	# Create a adress
	server_address = (address, port)
	
	sock.bind(server_address)
	sock.listen(1)

	sock.setblocking(0)
	return sock

def my_application(env, start_response):
	# first we should create a request object
	request = Request(env) 
	print (request.args) 
	text = 'Hello %s age is : %s !' % (request.args.get('name','World'), request.args.get('age', 14))
	response = Response(text, mimetype='text/plain')
	return response(env, start_response)

listener = tcp_listener(8888)
server = pywsgi.WSGIServer(listener, my_application)
server.serve_forever()


	
