#/usr/bin/env python

import logging
import logging.handlers
import SocketServer


class FileLogger(object):

	logformat =  '%(asctime)s %(levelname)s %(message)s'
	logport = 8020
	logfile = "neonserver.log"

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
