#/usr/bin/env python

import logging
import logging.handlers
import SocketServer


class FileLogger(object):

	logformat =  '%(asctime)s %(levelname)s %(message)s'
	logport = 8020
	logfile = "error.log"

	def __init__(self,type,logfile = None):
		if logfile is not None:
			self.logfile = logfile
			
		self.logger = logging.getLogger(type)
		formatter = logging.Formatter(self.logformat)
		fileHandler = logging.FileHandler(self.logfile)
		fileHandler.setFormatter(formatter)
		self.logger.addHandler(fileHandler)

	def debug(self,message):
		self.logger.setLevel(logging.DEBUG)
		self.logger.debug(message)
		return

	def info(self,message):
		self.logger.setLevel(logging.INFO)
		self.logger.info(message)
		return

	def error(self,message):
		self.logger.setLevel(logging.ERROR)
		self.logger.error(message)
		return

	def exception(self,message):
		self.logger.exception(message)
		return	

class SocketLogger(object):

	logformat =  '%(asctime)s %(levelname)s %(message)s'
	logport = 8020

	def __init__(self,type):
		self.logger = logging.getLogger(type)
		formatter = logging.Formatter(self.logformat)
		socketHandler = logging.handlers.SocketHandler('localhost',self.logport)
		socketHandler.setFormatter(formatter)
		self.logger.addHandler(socketHandler)

	def debug(self,message):
		self.logger.setLevel(logging.DEBUG)
		self.logger.debug(message)
		return

	def info(self,message):
		self.logger.setLevel(logging.INFO)
		self.logger.info(message)
		return

	def error(self,message):
		self.logger.setLevel(logging.ERROR)
		self.logger.error(message)
		return

class LogServer(SocketServer.BaseRequestHandler):

    logfile = "/var/log/neonserver.log"

    def handler(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024).strip()
        #print "{} wrote:".format(self.client_address[0])
        print self.data

	def start(self):
	    HOST, PORT = "localhost", 8020
	    server = SocketServer.TCPServer((HOST, PORT), self.handler)

	    # Activate the server; this will keep running until you
	    # interrupt the program with Ctrl-C
	    server.serve_forever()
