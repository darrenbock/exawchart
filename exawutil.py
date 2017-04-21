#!/usr/bin/python

#
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
#
#     NAME
#       exawutil.py
#
#     DESCRIPTION
#      Common utilities for exaw* modules
#      This contains the common constants for dictionary keys
#      and common functions
#
#     MODIFIED   (MM/DD/YY)
#     cgervasi    09/28/16 - add summary page
#     cgervasi    09/16/16 - refactor add_empty_point
#     cgervasi    08/15/16 - use template directory
#     cgervasi    08/03/16 - change name format
#     cgervasi    07/12/16 - move to JET
#     cgervasi    06/21/16 - fortify
#     cgervasi    05/16/16 - add support for multiple hosts
#     cgervasi    04/28/16 - add data for master slider
#     cgervasi    03/24/16 - Creation
#

import os
import errno
import gzip
from bz2 import BZ2File
from socket import getfqdn
from subprocess import Popen, PIPE
from datetime import timedelta,datetime
# from mimetypes import guess_type
import sys
from logging import NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL, traceback

try:
  paths = ['/exawchart/lib/python']
  for path in paths:
    if path not in sys.path:
      sys.path.append(path)
  import exalogger
except:
  exitcode = 1
  print '[ERROR]: Program error.  Supporting libraries not found.  Check installation.'
  sys.exit(exitcode)

# import sys

# expected date input format in command-line argument
DATE_FMT_INPUT  = '%m/%d/%Y %H:%M:%S'
JSON_DATE_FMT = '%Y-%m-%dT%H:%M:%S' # output format for Json conversion

# Constants
# file types
FILE_GZ   = 'gz'
FILE_BZ2  = 'bz2'
FILE_ZIP  = 'zip'
FILE_TEXT = 'text'
FILE_UNKNOWN = 'unknown'

# finding types
FINDING_TYPE_INFO = 'info'
FINDING_TYPE_SUMMARY = 'summary'
FINDING_TYPE_DETAIL = 'detail'
# dictionary keys used in buckets/bucket dictionary objects
# keys for each bucket dictionary object
TIMESTAMP = 'ts'
CPU   = 'cpu'
FLASH = 'flash'
DISK  = 'disk'

CNT  ='count' # count of samples in bucket for CPU and individual FLASH/DISKS

# keys for CPU in the bucket
USR  = 'usr'
NICE = 'nice'
SYS  = 'sys'
WIO  = 'wio'
STL  = 'stl'
IDL  = 'idl'
BUSY = 'busy'


# keys for individual disks
# FLASH/DISK are keyed by device name, values are dictionary objects
# with the following keys
RPS     = 'rps'
WPS     = 'wps'
RSECPS  = 'rsecps'
WSECPS  = 'wsecps'
AVGRQSZ = 'avgrqsz'
AVGQUSZ = 'avgqusz'
AWAIT   = 'await'
SVCTM   = 'svctm'
UTIL    = 'util'

# keys for data to be displayed
RMBPS    = 'rmbps'
WMBPS    = 'wmbps'
IOPS     = 'iops'
MBPS     = 'mbps'
VALUE    = 'value'


TITLE    = 'title'

SUMMARY  = 'summary'
AVG      = 'avg'
MAX      = 'max'

# maximum number of buckets - this controls chart resolution
DEFAULT_MAX_BUCKETS = 500

# default flash disks
DEFAULT_FLASH_DISKS = [ 'sdn', 'sdo', 'sdp', 'sdq',
                'sdr', 'sds', 'sdt', 'sdu',
                'sdv', 'sdw', 'sdx', 'sdy',
                'sdz', 'sdaa', 'sdab', 'sdac' ]

# default hard disks
DEFAULT_HARD_DISKS = [ 'sda', 'sdb', 'sdc', 'sdd',
               'sde', 'sdf', 'sdg', 'sdh',
               'sdi', 'sdj', 'sdk', 'sdl' ]

#------------------------------------------------------------
# line numbers in ExaWatcher file
# We expect the first few lines of the file the way ExaWatcher
# prints it.  If it doesn't conform to the expected format we will
# ignore the file

# we expect first line of the file, after the '###' to have Starting time
EXAWATCHER_STARTING_TIME_POSITION = 1
# line with sampling interval
EXAWATCHER_SAMPLE_INTERVAL_POSITION = 2
# line with archive count
EXAWATCHER_ARCHIVE_COUNT_POSITION = 3
# line with module
EXAWATCHER_MODULE_POSITION = 4
# line with colleciton command
EXAWATCHER_COLLECTION_COMMAND_POSITION = 5
# misc info in exawatcher header
EXAWATCHER_MISC_INFO_POSITION = 6
# # of header lines
EXAWATCHER_HEADER_LINES = 8 # # of lines for ExaWatcher header

# special dictionary keys to identify file types
magic_dict = {
  "\x1f\x8b\x08": FILE_GZ,
  "\x42\x5a\x68": FILE_BZ2,
  "\x50\x4b\x03\x04": FILE_ZIP
  }

#------------------------------------------------------------
# user-defined exceptions
class UnrecognizedFile(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

class DuplicateFile(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

class InvalidReportTime(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

class NoDataInFile(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

class HostNameMismatch(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

class MaxHostsExceeded(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

class UnknownFindingType(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

#------------------------------------------------------------
# read-only property for classes
def ro_property(field):
  return property(lambda self : self.__dict__[field])

#------------------------------------------------------------
# for managing report context:
# start time, end time, bucket info, directories and filenames
#------------------------------------------------------------
class ReportContext(object):
  # bucket intervals are multiples of _MIN_BUCKET_INTERVAL
  # we use 5 which is the default exawatcher
  # however, for cell metric history we use 60
  _DEFAULT_MIN_BUCKET_INTERVAL = 5
  # set read-only attributes for report context
  report_start_time = ro_property('_report_start_time')
  report_end_time = ro_property('_report_end_time')
  max_buckets = ro_property('_max_buckets')
  bucket_interval = ro_property('_bucket_interval')
  num_buckets = ro_property('_num_buckets')
  outdir = ro_property('_outdir')
  multihost = ro_property('_multihost')
  template_dir = ro_property('_template_dir')
  min_bucket_interval = ro_property('_min_bucket_interval')
  hostnames = ro_property('_hostnames')

  def __init__(self, log_level = WARNING):
    # create the logger
    self._logger = exalogger.Logging()
    self._logger.loginit('EXAWCHART')  # only stdout for now
    self._logger.logger.setLevel(log_level)
    # initialize the read-only attributes
    self._report_start_time = datetime.utcfromtimestamp(0)
    self._report_end_time = datetime.utcfromtimestamp(0)
    self._max_buckets = DEFAULT_MAX_BUCKETS
    self._min_bucket_interval = self._DEFAULT_MIN_BUCKET_INTERVAL
    self._bucket_interval = 0
    self._num_buckets = 0
    self._outdir = None
    self._multihost = False
    # keyed by hostname, each one mapping to a HostSummary object
    self._hostnames = { }
    
    # set template directory - should be a subdir under this module
    self._template_dir = os.path.join(os.path.realpath(os.path.dirname(__file__)),'templates')
    

  def __repr__(self):
    return 'report_start_time: %s, report_end_time: %s, max_buckets: %d, bucket_interval: %d, num_buckets: %d, outdir: %s, multihost: %s, template: %s' % (self._report_start_time,
                                   self._report_end_time,
                                   self._max_buckets,
                                   self._bucket_interval,
                                   self._num_buckets,
                                   self._outdir, 
                                   self._multihost,
                                   self._template_dir)

  #------------------------------------------------------------
  def log_msg(self, level, msg, exitcode = None):
    # only display messages based on level
#    if level.upper() == 'WARNING' or level.upper() == 'ERROR':
#      msg += '\n ->' + traceback.format_exc(1)
    getattr(self._logger.logger, level)('%s' % msg)

    # in case we want full stack
#     tb = ''
#     if level.upper() == 'ERROR':
#       formatted_tb = traceback.format_exc().splitlines()
#       for i in range(0, len(formatted_tb)):
#         tb += '\n -> %s' % formatted_tb[i]
#       getattr(self._logger.logger, level)('%s' % tb)
# 

#     self._logger.log_stdout(level,
#                             fixed_msg_part=msg, stdout_msg_part='',
#                             mincode='000', majcode='000',
#                             exitcode=exitcode)

  #------------------------------------------------------------
  def set_log_level(self, log_level):
    new_level = log_level.upper()
    if new_level == 'DEBUG':
      self._logger.logger.setLevel(DEBUG)
    elif new_level == 'INFO':
      self._logger.logger.setLevel(INFO)
    elif new_level == 'WARNING':
      self._logger.logger.setLevel(WARNING)
    elif new_level == 'ERROR':
      self._logger.logger.setLevel(ERROR)

  #------------------------------------------------------------
  def set_report_context(self,
                         start_time,
                         end_time,
                         max_buckets = DEFAULT_MAX_BUCKETS,
                         min_bucket_interval = _DEFAULT_MIN_BUCKET_INTERVAL,
                         outdir = None):

    # perform validation check here
    if start_time == datetime.utcfromtimestamp(0) or end_time == datetime.utcfromtimestamp(0):
      raise InvalidReportTime('start_time: %s, end_time: %s' % (start_time,end_time))
    # continue setting the attributes
    self._report_start_time = start_time
    self._report_end_time = end_time
    self._max_buckets = max_buckets
    self._min_bucket_interval = min_bucket_interval
    self._bucket_interval, self._num_buckets = self._set_bucket_interval()

    try:
      # set the output directory
      self._outdir = self._create_outdir(outdir)

    except:
      raise

  #------------------------------------------------------------
  def get_info(self):
    '''
    returns info required by HTML templates
    '''
    return self._report_start_time, self._report_end_time, self._num_buckets, self._bucket_interval, self._multihost

  #------------------------------------------------------------
  def get_json_object(self):
    '''
      returns object with fields ready for javascript usage
    '''
    return { "reportStartTime": datetime.strftime(self._report_start_time,
                                                  JSON_DATE_FMT),
             "reportEndTime"  : datetime.strftime(self._report_end_time,
                                                  JSON_DATE_FMT),
             "numBuckets"     : self._num_buckets,
             "bucketInterval" : self._bucket_interval }
  
  #------------------------------------------------------------
  def _set_bucket_interval(self):
    time_range = timedelta_get_seconds(self._report_end_time - self._report_start_time)
    bucket_interval = self._min_bucket_interval
    while(int(time_range/bucket_interval) > self._max_buckets):
      bucket_interval += self._min_bucket_interval
    num_buckets = int(time_range/bucket_interval) + 1
    return (bucket_interval,num_buckets)

  #------------------------------------------------------------
  def _create_outdir(self, outdir):
    '''
    attempts to create the directory specified in outdir
    if it exists, ignore the error; all other errors are raised
    '''
    # if nothing was specified by user, then use the current directory
    if outdir == None or outdir == '':
      outdir = os.getcwd()
    # normalize path
    outdir = os.path.normpath(outdir) # normalize path
    outdir = os.path.realpath(outdir) # canonical path

    # determine subdirectory
    # TODO: add duration to subdir
    subdir = self._get_subdir_name(outdir)

    # and append subdirectory to outdir
    outdir = os.path.join(outdir,subdir)

    try:
      os.makedirs(outdir)
      # TODO: do we need to set permissions here?
    except OSError as exception:
      if exception.errno == errno.EACCES:
        self.log_msg('error','Invalid permission on directory: %s (%s)' % (outdir, exception.strerror))
        raise
      if exception.errno != errno.EEXIST:
        self.log_msg('error','Unable to create directory: %s (%s)' % (outdir, exception.strerror))
        raise
    return outdir

  #------------------------------------------------------------
  def _get_report_duration(self):
    delta = self._report_end_time - self._report_start_time
    hours = int(delta.seconds/3600)
    mins = int(delta.seconds - hours*3600)/60
    seconds = delta.seconds - hours*3600 - mins*60
    if delta.days > 0:
      duration = '%dh%02dm%02ds' % (delta.days*24+hours, mins, seconds)
    else:  
      duration = '%02dh%02dm%02ds'% (hours, mins, seconds)

    return duration

  #------------------------------------------------------------
  def _get_subdir_name(self, outdir):
    # subdir name currently from time
    # TODO: add duration as well
    subdir = self._report_start_time.strftime('%Y_%m_%d_%H_%M_%S') + '_' + self._get_report_duration()
    suffix = 0
    while os.path.exists(os.path.join(outdir,subdir + '_' + str(suffix))):
      suffix += 1
    return subdir + '_' + str(suffix)
  
  #------------------------------------------------------------
  def get_bucket_id(self, sample_time):
    '''
    returns bucket_id of sample_time
    '''
    return int(timedelta_get_seconds(sample_time - self._report_start_time)/self._bucket_interval)

  #------------------------------------------------------------
  def bucket_id_to_timestamp(self,bucket_id):
    '''
    returns start time of bucket if bucket_interval == MIN_BUCKET_INTERVAL
    otherwise returns midpoiont of bucket
    NOTE: we could always just return the midpoint, but the confusion
    could be if the bucket_interval = 5 and bucket_start_times are
    aligned with ExaWatcher data, then the chart will be off by 2-3 s
    '''
    if self._bucket_interval == self._min_bucket_interval:
      bucket_time = self._report_start_time + timedelta(seconds=bucket_id*self._bucket_interval)
    else:
      bucket_time = self._report_start_time + timedelta(seconds=bucket_id*self._bucket_interval + self._bucket_interval/2)
    return bucket_time

  #------------------------------------------------------------
  def set_multihost(self, value):
    '''
      sets the multihost variable
    '''
    self._multihost = value

  #------------------------------------------------------------
  def add_html_file(self, hostname, stattype, file_tuple, pos = None, filetype='summary' ):
    '''
      adds the file_tuple for the hostname in the hostnames object
      The file tuple is in the html_files field of the HostSummary
      object.
      If the stattype is summary, then this is the page summary
      which is a field in HostSummary object.  All other
      stattypes have their own list for the html files.
      
      hostname: hostname information for the file, empty string for multicell
                files
      stattype: stattype of this html file_tuple (either iostat, mpstat,
                cellsrvstat, alerts or summary)
      file: is a tuple of (filename, title)
              title is really type of chart, i.e. IO Summary, etc.
    '''
    if hostname not in self._hostnames:
      self.add_hostinfo(hostname)
      
    # the summary html file is not embedded in a stat object
    # it is a toplevel summary for this host
    if stattype == 'summary':
      self.hostnames[hostname].set_summary_html(file_tuple)
    else:
      host_stat = getattr(self.hostnames[hostname], stattype.lower())
      host_stat.add_html_file( file_tuple, pos)

  #------------------------------------------------------------
  def write_html_file(self, filename, title, htmlstr):
    '''
      writes htmlstr into specified filename
      constructs filename:
        <filename_identifier>.html
    '''
    output = ( None , None )
    try:
      owd = os.getcwd()
      os.chdir(self._outdir)
      datafile = open(filename,'w')
      datafile.write(htmlstr)
    except Exception as e:
      self.log_msg('error', 'Error in writing file %s (%s)' %(self._outdir + '/' + filename, str(e)))

    else:
      output = (filename, title)
      self.log_msg('info', 'Generated file: %s' % (self._outdir + '/' + filename))
    finally:
      datafile.close()
      #TODO: should we set permission here
      os.chdir(owd)
      
    return output

  #------------------------------------------------------------
  def add_hostinfo(self, hostname):
    '''
      checks if hostname exists in the hostnames dictionary object
      and adds if it necessary.  It should be a HostSummary object
    '''
    if hostname not in self._hostnames:
      self._hostnames[hostname] = HostSummary(hostname)

  #------------------------------------------------------------
  def num_html_files(self):
    '''
      retrieves number of html files produced
    '''
    cnt = 0
    for host in self._hostnames:
      cnt += self._hostnames[host].num_html_files()

  #------------------------------------------------------------
  def get_html_files(self):
    '''
      retrieves a list of html files (and titles) produced for all hosts.
      keyed by hostname
    '''
    html_files = {}
    for host in self._hostnames:
      html_files[host] = self._hostnames[host].html_files()
    return html_files

#------------------------------------------------------------
class HostMetadata(object):
  '''
    metadata object for a host.  This has the list of files processed for
    the host, and is extended by the parsing routines to add information
    as needed
  '''
  name = ro_property('_name')
  def __init__(self,hostname):
    self._name = hostname
    self.processed_files = []   # list of processed files

  def __str__(self):
    return 'name: %s, processed_files: %s' % (self._name, str(self.processed_files) )

#------------------------------------------------------------
class StatFileSummary(object):
  '''
    summary information for the file(s) processed
    which has a summary of the stats, findings and html files produced
  '''
  # read-only attributes
  stattype = ro_property('_stattype')
  num_findings = ro_property('_num_findings')
  
  def __init__(self,name):
    self._stattype = name
    self.summary_stats = {}  # summary bucket
    self.findings = []       # array of findings
    self.html_files = []     # list of tuples; should this be ro property?
    # summary information about number of findings
    # this is used to determine if we need a marker by the html file
    self._num_findings = { FINDING_TYPE_INFO   : 0,
                           FINDING_TYPE_SUMMARY: 0,
                           FINDING_TYPE_DETAIL : 0 }

  def __str__(self):
    return 'summary_stats: %s, findings: %s, html_files %s' % (self.summary_stats, self.findings, self.html_files)

  #------------------------------------------------------------
  def add_html_file(self, file_tuple, pos = None ):
    '''
      appends to the html files array
    '''
    if pos == None:
      self.html_files.append(file_tuple)
    else:
      self.html_files.insert(pos, file_tuple)

  #------------------------------------------------------------
  def add_finding(self, msg, finding_type = FINDING_TYPE_SUMMARY):
    '''
      appends a finding, and increments the number of findings
      based on the finding type
    '''
    self.findings.append(msg)
    if finding_type.lower() in self._num_findings:
      self._num_findings[finding_type.lower()] += 1
    else:
      raise UnknownFindingType(finding_type)

  #------------------------------------------------------------
  def html_files_with_markers(self):
    '''
      retrieves array of html files, but also adds a marker
      ('True') if there is a finding associated with that file
    '''
    # Note, in order to properly distinguish between summary and detail
    # findings (which only exists for iostat), we assume the title of
    # used as the menu entry has 'Summary' and 'Detail' in it.
    # otherwise, we will need to start categorizing html files as well
    file_tuples = []
    for (f,t) in self.html_files:
      file_tuple = (f,t)
      # if we have findings
      if self._num_findings[FINDING_TYPE_SUMMARY] > 0:
        # for iostat, only mark the summary page, all other stattypes
        # there should only be one page, so we mark it
        if (self._stattype == 'iostat' and 'Summary' in t) or (self._stattype != 'iostat'):
          file_tuple += (True, )

      # currently, only iostat has detail findings so we mark it here    
      if self._num_findings[FINDING_TYPE_DETAIL] > 0 and self._stattype == 'iostat' and 'Detail' in t:
        file_tuple += (True, )
                              
      file_tuples.append(file_tuple)
    return file_tuples  

#------------------------------------------------------------
class HostSummary(object):
  '''
    contains information about a host, specifically summarized information
    of the files that it has processed.
    Each filetype is a property of this object.
    In addition, the summary_html will have the (file, title) of the
    summary page that gets generated which has consolidated information
    from all files
  '''
  # read-only properties
  name = ro_property('_name')
  summary_html = ro_property('_summary_html')
  
  def __init__(self,hostname):
    self._name = hostname
    self.iostat = StatFileSummary('iostat')
    self.mpstat = StatFileSummary('mpstat')
    self.cellsrvstat = StatFileSummary('cellsrvstat')
    self.alerts = StatFileSummary('alerts')
    self._summary_html = ( None, None ) # single page summary for this host

    # array of stattypes; can potentially just use python dir to get this
    self._stattypes = [ 'iostat', 'mpstat', 'cellsrvstat', 'alerts' ]
    
  def __str__(self):
    return 'name: %s, iostat: %s, mpstat: %s, cellsrvstat: %s, alerts: %s' % (self._name, self.iostat, self.mpstat, self.cellsrvstat, self.alerts)

  
  #------------------------------------------------------------
  def set_summary_html(self, file_tuple):
    '''
      setter for _summary_html
    '''
    self._summary_html = file_tuple

  #------------------------------------------------------------
  def get_first_chart(self):
    '''
      retrieves first chart produced (html file only, not the tuple)
      This is so we know the landing page for the frame.
      If the summary exists, we use that; otherwise we go through the
      stats to determine which one a file we can use
    '''
    first_file = None
    if self._summary_html != ( None, None ):
      first_file = self._summary_html[0]
    # the rest are arrays of tuples, we only want the html file
    else:
      for stattype in self._stattypes:
        self_stattype = getattr(self, stattype)
        if len(self_stattype.html_files) > 0:
          first_file = self_stattype.html_files[0][0]
          break
    return first_file

  #------------------------------------------------------------
  def num_html_files(self):
    '''
      return number of html files produced for this host
      this is a simple check to make sure if we had files produced
      TODO: should we just return a boolean
    '''
    return len(self.iostat.html_files) + len(self.mpstat.html_files) + len(self.cellsrvstat.html_files) + len(self.alerts.html_files)

  #------------------------------------------------------------
  def html_files(self):
    '''
      return an array of html files produced, including summary
    '''
    html_files = []
    if self._summary_html != ( None, None):
      html_files.append(self._summary_html)
    for stattype in self._stattypes :
      self_stattype = getattr(self, stattype)
      if len(self_stattype.html_files) > 0:
        # note: do not append, as each one is an array
        html_files += self_stattype.html_files_with_markers()
    return html_files


#------------------------------------------------------------
# Helper functions

#------------------------------------------------------------
def file_type(filename, report_context):
  '''
    determine filetype for the given filename - we check for bz2, gz and zip
    all other files are assumed to be text
    . should we close file here too?
  '''

  # first check this is a regular file
  if not(os.path.isfile(filename)):
    return FILE_UNKNOWN

  ftype = FILE_UNKNOWN
  max_len = max(len(x) for x in magic_dict)

  try:
    f = open(filename, 'r')
    file_start = f.read(max_len)
  except Exception as e:
    ftype = FILE_UNKNOWN
    report_context.log_msg('warning','Unable to determine filetype: %s [%s]' % (fname, str(e)))
    return ftype
  else:
    for magic, filetype in magic_dict.items():
      if file_start.startswith(magic):
        return filetype

  # last check if this is text
  # first alternative is to use MIME_TYPES, but this is based on suffix of
  # filename, so not very good
  # (mtype, encoding) = guess_type(filename)
  # if mtype == None and encoding == None:
  #   ftype = FILE_TEXT
  #
  # second alternative is to use the file command, an
  # only consider if it is plain text - no shell scripts, html, etc.
  # this may be too strict ... but we'll see this only handles it if
  # user have uncompress'd the file anyway

  try:
    p = Popen(['file','-bi',filename], stdin=PIPE, stdout=PIPE, shell=False)
    (output,err) = p.communicate()
    rc = p.returncode
    
    if rc == 0 and output.startswith('text/plain'):
      ftype = FILE_TEXT
  except Exception as e:
    ftype = FILE_UNKNOWN
    report_context.log_msg('warning','Unable to determine filetype: %s' % fname)
  finally:  
    return ftype

#------------------------------------------------------------
def open_file(filename, filetype):
  '''
    opens the filetype based on the given filetype and returns
    the file descriptor
  '''
  input_file = None
  try:
    if filetype == FILE_BZ2:
      input_file = BZ2File(filename,'r')
    elif filetype == FILE_GZ:
      input_file = gzip.open(filename,'r')
    elif filetype == FILE_ZIP:
      # zip is an archive with list of files
      zflist = zipfile.ZipFile(filename,'r')
      # not a valid exawatcher filetype
      input_file = None
    elif filetype == FILE_TEXT:
      # to do check for actual real text
      input_file = open(filename,'r')
  except:
    raise
  finally:
    return input_file

#------------------------------------------------------------
def timedelta_get_seconds(delta):
   '''
   returns number of seconds in a delta (a timedelta) object
   NOTE: in python 2.7 we can use delta.total_seconds()
   '''
   return delta.days*86400 + delta.seconds

#------------------------------------------------------------
def get_file_end_time(file_start_time,
                      sample_interval_line,
                      archive_count_line):
  sample_interval = int(sample_interval_line.strip().rsplit()[-1])
  archive_count = int(archive_count_line.strip().rsplit()[-1])
  return file_start_time + timedelta(seconds = sample_interval*archive_count)

#------------------------------------------------------------
def get_hostname():
  return getfqdn()

#------------------------------------------------------------
def get_hostname_from_filename(fname):
  try:
    h1 = fname.split('_')[-1]
    # now get the substring until the .dat
    return h1[0:h1.index('.dat')]
  except ValueError as e:
    # if we can't get it from filename, return the hostname
    get_hostname()
  except Exception as e:
    raise

#------------------------------------------------------------
def validate_disk_list(disks):
  '''
  all items in disks should be an expected disk name - e.g. sd* or nvm*
  and should only contain a-z and 0-9
  '''
  for disk in disks:
    if disk[0:4] != 'nvme' and disk[0:2] != 'sd':
      return False
  return True

def validate_disk(disk):
  if disk[0:4] == 'nvme' or disk[0:2] == 'sd':
    # fortify: return at most 10 characters
    return disk[0:10]
  return None

#------------------------------------------------------------
def add_empty_point(datalist,
                    pos = None):
  '''
    traverse datalist and add the empty point to the lists
    This is required so that the charts have the correct start/end points
  '''
  # only add it if list is not empty - i.e. for empty charts, don't bother
  # since we won't have any datapoints
  if type(datalist) == list and len(datalist) > 0:
    if pos != None:
      datalist.insert(pos, None )
    else:
      datalist.append( None )
  else:
    for attr in datalist:
      add_empty_point(datalist[attr],pos)


#------------------------------------------------------------
def add_start_end_times(report_context,
                         buckets,
                         xAxis,
                         data):
  '''
    adds start/end data points if they do not exist, so that the chart
    x-axis (of time) has correct range.  This calls add_empty_points()
    to add the end points to all lists in data.

    PARAMETERS:
      report_context: ReportContext with metadata info, e.g. bucket_interval,
                      report_start_time/report_end_time, etc.
      buckets: parsed iostat data from iostat file(s).  
      xAxis: array/list containing timestamps
      data: buckets data converted into an object of arrays, where each
            array contains the series (this is the format for use by
            JET).
    NOTE:
      This routine adds the start/end points to the xAxis, so make sure
      this is only called once per chart.
  '''

  # add start point
  if 0 not in buckets:
    xAxis.insert(0,report_context.bucket_id_to_timestamp(0).strftime(JSON_DATE_FMT) )
    # add empty points to all arrays in data
    add_empty_point(data, 0)

  # add end point; determine last bucket_id based on report_end_time
  last_bucket_id = report_context.get_bucket_id(report_context.report_end_time)
  if last_bucket_id not in buckets:
    xAxis.append(report_context.bucket_id_to_timestamp(last_bucket_id).strftime(JSON_DATE_FMT))
    add_empty_point(data, None)


