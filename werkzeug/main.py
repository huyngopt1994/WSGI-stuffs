


import urlparse 
import socket

from gevent import pywsgi

from shortly.site import create_app


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

if __name__ == '__main__':
	my_app = create_app()

	listener = tcp_listener(8888)

	server = pywsgi.WSGIServer(listener, my_app)
	server.serve_forever()


	
