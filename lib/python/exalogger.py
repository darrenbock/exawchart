#!/usr/bin/python
#
# exalogger.py
#
# Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.
#
#    NAME
#      exalogger.py
#
#    DESCRIPTION
#      Provides a standard method of logging and tracing
#      for python code in Exadata.
#
#    NOTES
#      Please see description below.
#
#    MODIFIED   (MM/DD/YY)
#    ksimmons    01/28/14 - Do in-memory message retrieval
#    ksimmons    01/13/14 - Added standard message lookup from a resource bundle
#    ksimmons    01/13/13 - Creation
#

import inspect
import os
import sys
import logging
import logging.handlers
import time
import trace
try:
  from image_messages import message_dict
except:
  pass


## DESCRIPTION:
## LOGGING:
## Standardize the format of logged messages and simplify
## the logging from python applications in Exadata code.
## This module uses the logging module from the
## python standard library.
## TRACING:
## Provides a simpler mechanism to generate traces of
## selected python code, either per command or function
## or for multiple functions. Usage follows logging info.
## Uses the trace module from the python standard library.

## LOGGING USAGE:
## import this module (exalogger) and initialize it's use with
## app, logfile, console, maxBytes, and backupCount options, then use the logger to log a message.
## ARGUMENTS:
## app            # Application or facilty name (arbitrary text), default is ""
## logfile        # File to write to, default is None.
##                # If logfile is not specified then output will be to stdout regardless of console setting
## console        # True|False, True = output to stdout, default is False. Will output to both stdout and file.
## maxBytes       # Max logfile size in bytes, for file rotation, default is 0 (NO ROTATE)
## backupCount    # Number of backup logfiles to keep, default is 5
##                # File rotation is off by default. If maxBytes is specified then logfile will be rotated
##                # when maxBytes size is approached. The backup logfiles are named appended with
##                # an index i.e. logfile.1, logfile.2, etc
##
## LOGGING LEVELS:
## function            log level  numerical
## logger.critical()   CRITICAL     50
## logger.error()      ERROR        40
## logger.warning()    WARNING      30
## logger.info()       INFO         20
## logger.debug()      DEBUG        10
## Also:
## logger.exception()  ERROR        40 (also captures traceback of exception)
##
## EXAMPLES:
## import exalogger                                                 # Import the module
## l = exalogger.Logging()                                          # Instantiate the Logging class
## l.loginit(app="myapp", logfile="./myapp.log", console=True)      # Initialize, both logfile and stdout, no logfile rotation
## l.loginit(app="myapp", logfile="./myapp.log", maxBytes=30000)    # Initialize, file only, rotate logfiles at 30Kbytes, keep 5 backups
## l.loginit(app="myapp")                                           # Initialize, stdout only
##                                                                  # Logging messages:
## l.logger.info('Informational message')                           # Logging an INFO message
## l.logger.warning('Warning message')                              # Logging a WARNING message
##                                                                  # Exception logging and tracebacks:
## l.logger.exception('Exception message')                          # Only use this in an actual exception as traceback info is expected

## TRACING USAGE:
## ARGUMENTS:
## tracefile      # File to write to, default is None.
##                # If logfile is not specified then output will be to stdout regardless of console setting
## console        # True|False, True = output to stdout, default is False. Tracing output can only go to one destination
##                # If console is True then the tracefile will not be used
## EXAMPLES:
## import exalogger                                                 # Import the module
## t = exalogger.Tracing()                                          # Instantiate the Tracing class
## t.traceinit(tracefile="myapp.trc", console=False)                # Initialize the tracer
## t.tracer.run('func_to_anaylze()')                                # Run a trace on a function
## t.tracer.run('main()')                                           # Tracing multiple (possibly) functions
## t.tracer.run('print "Hello World!"')                             # Trace a command
## t.traceclose(tracefile="myapp.trc")                              # Must close the tracing run if not using stdout


class Logging(object):
  def __init__(self):
    pass

  def exadata_time(self, record, datefmt=None):
    return time.strftime('%Y-%m-%d %H:%M:%S %z')

  def get_lineno(self):
    return inspect.currentframe().f_back.f_lineno

  def loginit(self, app=None, logfile=None, console=False, f_lineno=False, maxBytes=0, backupCount=5):
    """Pass the facility name (app) and logfile destination
       Log the message to the selected output streams
    """
    if app == None or app == "":
      app="--"
    self.logger = logging.getLogger(app)
    self.logger.setLevel(logging.DEBUG)
    logging.Formatter.formatTime = self.exadata_time
    self.fh_formatter = logging.Formatter(fmt='[%(created).0f][%(asctime)s][%(levelname)-7s][%(filename)s][%(name)-12s][%(message)s]')
    self.ch_formatter = logging.Formatter(fmt='[%(asctime)s][%(levelname)-7s][%(filename)s][%(name)-12s][%(lineno)d][%(message)s]')
    self.fh_lineno_formatter = logging.Formatter(fmt='[%(created).0f][%(asctime)s][%(levelname)-7s][%(filename)s][%(name)-12s][%(lineno)d][%(message)s]')
    if logfile == None or console == True:
      ch = logging.StreamHandler()
      ch.setLevel(logging.DEBUG)
      ch.setFormatter(self.ch_formatter)
      self.logger.addHandler(ch)
    if logfile and f_lineno == True:
      fh = logging.handlers.RotatingFileHandler(logfile, maxBytes=maxBytes, backupCount=backupCount)
      fh.setLevel(logging.DEBUG)
      fh.setFormatter(self.fh_formatter)
      self.logger.addHandler(fh)
      noconsole = False
    if logfile and f_lineno == False:
      fh = logging.handlers.RotatingFileHandler(logfile, maxBytes=maxBytes, backupCount=backupCount)
      fh.setLevel(logging.DEBUG)
      fh.setFormatter(self.fh_formatter)
      self.logger.addHandler(fh)
      noconsole = False

  def get_rsc_fixed_message_part(self, majcode, mincode):
    """ Retreive standard message from majcode-mincode lookup """
    key = 'IMG-%s-%s' % (majcode, mincode)
    if 'image_messages' in sys.modules:
      if 'message_dict' in globals() and message_dict.has_key(key):
        rsc_fixed_msg_part = message_dict[key]
      else:
        rsc_fixed_msg_part = None
    else:
      rsc_fixed_msg_part = None
    return rsc_fixed_msg_part

  def log_stdout(self, loglevel, fixed_msg_part, stdout_msg_part, majcode=None, mincode=None, exitcode=None, quiet=False, clicall=False):
    """ process user info and exit (if indicated) """
    if not quiet:
      if clicall:
        ##print 'IMG-%s-%s %s' % (majcode, mincode, stdout_msg_part)
        print '[IMG-%s-%s] %s %s' %  (majcode, mincode, fixed_msg_part, stdout_msg_part)
      else:
        print '[%s] [%s] [IMG-%s-%s] %s %s' % (time.strftime('%Y-%m-%d %H:%M:%S %z'), loglevel.upper(), majcode, mincode, fixed_msg_part, stdout_msg_part)



class Tracing(object):
  def __init__(self):
    self.tracer = trace.Trace(ignoredirs=[sys.prefix, sys.exec_prefix], trace=True, count=False)

  def traceinit(self, tracefile=None, console=False):
    if tracefile == None or console == True:
      self.log = None
    elif tracefile:
      self.log = open(tracefile, "a")
      sys.stdout = self.log

  def traceclose(self, tracefile=None, console=True):
    if self.log:
      os.path.isfile(tracefile) and self.log.close()
    sys.stdout = sys.__stdout__
