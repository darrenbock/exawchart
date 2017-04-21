#!/usr/bin/python

#
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
#
#     NAME
#       exawparse_cs.py
#
#     DESCRIPTION
#       Parses cellsrvstat data and creates a set of buckets
#
#     MODIFIED   (MM/DD/YY)
#     cgervasi    11/07/16 - check_zero
#     cgervasi    09/28/16 - add summary page
#     cgervasi    08/25/16 - add metric_list to handle missing data
#     cgervasi    06/21/16 - fortify
#     cgervasi    05/16/16 - add support for multiple hosts
#     cgervasi    04/05/16 - Creation
#
#     NOTES:
#       if the format of the files change, especially the timestamp
#       this will have to be modified

import sys

from datetime import datetime,timedelta
from exawutil import DEFAULT_MAX_BUCKETS, TIMESTAMP, VALUE, CNT, TITLE, EXAWATCHER_STARTING_TIME_POSITION, EXAWATCHER_SAMPLE_INTERVAL_POSITION, EXAWATCHER_ARCHIVE_COUNT_POSITION, EXAWATCHER_MODULE_POSITION, EXAWATCHER_COLLECTION_COMMAND_POSITION, EXAWATCHER_MISC_INFO_POSITION, EXAWATCHER_HEADER_LINES, DATE_FMT_INPUT, FILE_UNKNOWN, file_type, open_file, get_file_end_time, get_hostname_from_filename, UnrecognizedFile, DuplicateFile, NoDataInFile, HostNameMismatch, ReportContext, HostMetadata

import exawrules

#------------------------------------------------------------
# For parsing the files we need to group into buckets so we do not
# have too many data points. This is similar to exawparse_io
#
# Structure of buckets
# . buckets: dictionary object keyed by bucket_id
#            in effect, an array where the bucket_id is the index
#            into the array (we don't use python list in case
#            there are gaps in the timeline, the index will not be
#            correct).
#            Each bucket_id corresponds to a bucket dictionary object
#            (i.e. a struct)
# . bucket:
#     <bucket_id>: { <host>:
#                     { (<group_key>_<metric_key>): { VALUE: <x>, COUNT: <x> },
#                       (<group_key>_<metric_key>): { VALUE: <x>, COUNT: <x> },
#                     }
#                  }
#
# On the first pass, the VALUE will contain the SUMs for all the lines
# read, while COUNT has the number of samples.  We convert METRIC_DELTA
# into per second rates (using the interval seen in the cellsrvstat file)
# and any unit conversions (KB to GB) specified in METRIC_METADATA
#
# On the second pass, we calculate the averages for the bucket
#
# note that a (<group_key>_<metric_key>) is a generated key which we
# use to flatten out the structure.
#
# The consumers of this data (chart or plot) will need to do the grouping
# of various metrics as specified in the metadata by choosing the
# (group,metric) that it needs to plot.

#------------------------------------------------------------
# extend HostMetadata to include information for cellsrv stats for
# multi-host support
class HostMetadataCellSrvStat(HostMetadata):
  def __init__(self, hostname):
    super(HostMetadataCellSrvStat, self).__init__(hostname)
    self.check_zero = {}  # if all zero, we can suppress chart
    self._initialize_check_zero()
    self.metric_keys = [] # for metrics that we saw in the data
  def __str__(self, hostname):
    str = super(HostMetadataCellSrvStat, self).__str__()
    return str + ', metric_keys: %s, check_zero: %s ' % (str(self.metric_keys),
                                                         str(self.check_zero))

  def _initialize_check_zero(self):
    '''
      initializes the check_zero dictionary object with the keys
      so we know which metrics we need to hide if values are all 0
    '''
    for (group_name, group_metadata) in METRIC_METADATA.iteritems():
      gkey = group_metadata[KEY]
      for (metric_name, metric_metadata) in group_metadata[METRIC_LIST].iteritems():
        mkey = metric_metadata[KEY]
        key = generate_key(gkey,mkey)
        if CHECK_ZERO in metric_metadata and metric_metadata[CHECK_ZERO]:
          if key not in self.check_zero:
            self.check_zero[key] = 0


#------------------------------------------------------------
# Globals - initialize
buckets = {}
hostnames = {}

# this is created by caller
_my_report_context = None

EXAWATCHER_CELLSRVSTAT_MODULE_NAME = 'CellSrvStatExaWatcher'

#------------------------------------------------------------
# Constants
# Groups in the cellsrvstat file
GROUP_TS      = '===Current Time==='
GROUP_IO      = '== Input/Output related stats =='
GROUP_MEMORY  = '== Memory related stats =='
GROUP_EXEC    = '== Execution related stats =='
GROUP_NETWORK = '== Network related stats =='
GROUP_SIO     = '== SmartIO related stats =='
GROUP_FC      = '== FlashCache related stats =='
GROUP_FFI     = '== FFI related stats =='
GROUP_BIO     = '== LinuxBlock IO related stats =='

# keys for metric metadata
KEY = 'key'
GROUP  = 'group'
METRIC = 'metric'
METRIC_LIST = 'metric_list'
METRIC_TYPE = 'metric_type'
METRIC_DELTA  = 'delta'
METRIC_CURRENT = 'current'
STAT_UNIT     = 'statunit'
DISP_UNIT     = 'dispunit'
CHECK_ZERO    = 'check0'

CHART_GROUP   = 'chart'

# map of  chart id : title to display on chart
# any chart group used by METRIC_METADATA should be defined here
# hack to make sure we display thse ones first 
CHART_GROUP_IDS = {  'a01'   : 'Flash Cache Reads',
                     'a02'   : 'Flash Cache Read (Bytes)',
                     'a03'   : 'Flash Cache Writes',
                     'a04'   : 'Flash Cache Write (Bytes)',
                     'a05'   : 'Flash Cache Internal IO',
                     'a06'   : 'Flash Cache Internal IO (Bytes)',
                     'a07'   : 'Flash Cache Size',
                     'mem_alloc_failures': 'Memory Allocation Failures',
                     'a08'   : 'Smart I/O',
                     'a09'   : 'Smart I/O Passthru' }

#
# METRIC_METADATA lists the metrics that we want to chart/collect
# this is keyed by the GROUP
#
# KEY: short-hand key for the metric group, we use this for indexing
#      into our dictionaries, and in the javascript code that gets generated
# TITLE: user-friendly title used for the charts, if not displayed
#      uses the name
# METRIC_LIST: dictionary object with the metrics, keyed by the string
#      that we see in cellsrvstat output
# each metric object has
#    METRIC_TYPE: <delta|current>: determines which values we get from
#                                  cellsrvstat;
#                                  note: deltas are converted to per
#                                  second rates
#    KEY: short-hand key for the metric name, used as dictionary keys
#      and in javascript code that gets generated
#    TITLE: user-friendly title used for the charts, if not set uses
#      name
#    STAT_UNIT: unit in source data
#    DISP_UNIT: unit to display in charts - we will convert from STAT_UNIT
#               to DISP_UNIT. DELTAs will automatically add '/s'
#    CHECK_ZERO: if True, checks if all values are 0, we do not display
#               the chart and instead put a notation at the footer
#               Note, if metric belongs to a CHART_GROUP, and it is marked
#               as CHECK_ZERO: True, then all metrics in that CHART_GROUP
#               have to be marked CHECK_ZERO: True
#    CHART_GROUP: if set, then this will be displayed along with other
#               stats in a chart group
#               The chart group IDs and titles are in CHART_GROUP
#
# . grouping of multiple stats in a chart - will show up as multiple series
#
# NOTE: group key and metric key will be used to generate a single key
# for javascript code.  So make sure we don't inadvertently introduce
# duplicates when they're concatenated using gkey_mkey
#
METRIC_METADATA = { GROUP_IO: { KEY: 'io',  TITLE: 'I/O',
              METRIC_LIST: {}
                      },
            GROUP_MEMORY: { KEY: 'mem', TITLE: 'Memory',
              METRIC_LIST: { 'SGA heap used - cellsrv statistics - KB':
                             { METRIC_TYPE: METRIC_CURRENT,
                               KEY:         'sgaheap',
                               STAT_UNIT:   'KB',
                               DISP_UNIT:   'GB'},
                             'OS memory allocated to cellsrv (KB)':
                             { METRIC_TYPE: METRIC_CURRENT,
                               KEY:         'osmem',
                               STAT_UNIT:   'KB',
                               DISP_UNIT:   'GB'},
                             'OS memory allocated to offload groups (KB)':
                             { METRIC_TYPE: METRIC_CURRENT,
                               KEY:         'oflmem',
                               STAT_UNIT:   'KB',
                               DISP_UNIT:   'GB'},
                             'Number of allocation failures in 512 bytes pool':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY:         'fail512',
                               CHECK_ZERO:  True,
                               CHART_GROUP: 'mem_alloc_failures'},
                             'Number of allocation failures in 2KB pool':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY:         'fail2K',
                               CHECK_ZERO:  True,
                               CHART_GROUP: 'mem_alloc_failures'},
                             'Number of allocation failures in 4KB pool':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY:         'fail4K',
                               CHECK_ZERO:  True,
                               CHART_GROUP: 'mem_alloc_failures'},
                             'Number of allocation failures in 8KB pool':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY:         'fail8K',
                               CHECK_ZERO:  True,
                               CHART_GROUP: 'mem_alloc_failures'},
                             'Number of allocation failures in 16KB pool':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY:         'fail16K',
                               CHECK_ZERO:  True,
                               CHART_GROUP: 'mem_alloc_failures'},
                             'Number of allocation failures in 32KB pool':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY:         'fail32K',
                               CHECK_ZERO:  True,
                               CHART_GROUP: 'mem_alloc_failures'},
                             'Number of allocation failures in 64KB pool':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY:         'fail64K',
                               CHECK_ZERO:  True,
                               CHART_GROUP: 'mem_alloc_failures'},
                             'Number of allocation failures in 1MB pool':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY:         'fail1M',
                               CHECK_ZERO:  True,
                               CHART_GROUP: 'mem_alloc_failures'}
                           }
                         },
            GROUP_EXEC: {KEY: 'exec', TITLE: 'Execution',
              METRIC_LIST: {}
                        },
            GROUP_NETWORK: { KEY: 'net', TITLE: 'Network',
              METRIC_LIST:  { 'Number of active sendports':
                             { METRIC_TYPE:  METRIC_CURRENT,
                               KEY:          'actport' }
                            }
                          },
            GROUP_SIO: { KEY: 'sio', TITLE: 'Smart I/O',
              METRIC_LIST: { 'Total smart IO to be issued (KB)':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY: 'elig',
                               TITLE: 'sio issued',
                               CHECK_ZERO: True,
                               CHART_GROUP: 'a08',
                               STAT_UNIT:  'KB',
                               DISP_UNIT:  'GB' },
                             'Total smart IO filtered in send (KB)':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY: 'fsend',
                               TITLE: 'sio filtered in send',
                               CHECK_ZERO: True,
                               CHART_GROUP: 'a08',
                               STAT_UNIT:  'KB',
                               DISP_UNIT:  'GB' },
                             'Total smart IO read from flash (KB)':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY: 'rflash',
                               TITLE: 'sio from flash',
                               CHECK_ZERO: True,
                               CHART_GROUP: 'a08',
                               STAT_UNIT:  'KB',
                               DISP_UNIT:  'GB' },
                             'Total smart IO read from hard disk (KB)':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY: 'rdisk',
                               TITLE: 'sio from disk',
                               CHECK_ZERO: True,
                               CHART_GROUP: 'a08',
                               STAT_UNIT:  'KB',
                               DISP_UNIT:  'GB' },
                             'Total cpu passthru output IO size (KB)':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY: 'cpu',
                               TITLE: 'sio cpu passthru',
                               CHECK_ZERO: True,
                               CHART_GROUP: 'a09',
                               STAT_UNIT:  'KB',
                               DISP_UNIT:  'GB' },
                             'Total passthru output IO size (KB)':
                             { METRIC_TYPE: METRIC_DELTA,
                               KEY: 'pt',
                               TITLE: 'sio passthru',
                               CHECK_ZERO: True,
                               CHART_GROUP: 'a09',
                               STAT_UNIT:  'KB',
                               DISP_UNIT:  'GB' }
                           }
                       },
            GROUP_FC: { KEY: 'fc', TITLE: 'Flash Cache',
              METRIC_LIST: {'Number of read hits' :
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rhit',
                              TITLE:       'Read Hits',
                              CHECK_ZERO:  True,
                              CHART_GROUP: 'a01'},
                            'Number of read misses' :
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rmiss',
                              TITLE:       'Read Misses',
                              CHECK_ZERO:  True,
                              CHART_GROUP: 'a01'},
                            'Number of keep read hits' :
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rkhit',
                              TITLE:       'Keep Hits',
                              CHART_GROUP: 'a01',
                              CHECK_ZERO:  True },
                            'Number of keep read misses' :
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rkmiss',
                              TITLE:       'Keep Misses',
                              CHART_GROUP: 'a01',
                              CHECK_ZERO:   True},
                            'Number of reads attempted in  Columnar Cache':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rcchit',
                              TITLE:       'CC Read Hits',
                              CHART_GROUP: 'a01',
                              CHECK_ZERO:  True },
                            'Number of keep read hits from columnar cache':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rcckphit',
                              TITLE:       'CC Keep Reads',
                              CHART_GROUP: 'a01',
                              CHECK_ZERO:  True },
                            'Number of no cache reads':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rnocache',
                              TITLE:       'No Cache Reads',
                              CHART_GROUP: 'a01',
                              CHECK_ZERO:   True },
                            # flash cache read bytes
                            'Read on flashcache hit(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rby',
                              TITLE:       'Read Hit (Bytes)',
                              CHECK_ZERO:  True,
                              CHART_GROUP: 'a02',
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'MB'},
                            'Total IO size for read miss(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rmissby',
                              TITLE:       'Read Misses (Bytes)',
                              CHECK_ZERO:  True,
                              CHART_GROUP: 'a02',
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'MB' },
                            'Read on flashcache keep hit(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rkhitby',
                              TITLE:       'Keep Hits (Bytes)',
                              CHECK_ZERO:   True,
                              CHART_GROUP: 'a02',
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'MB'},
                            'Total IO size for keep read miss(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rkmissby',
                              TITLE:       'Keep Misses (Bytes)',
                              CHECK_ZERO:  True,
                              CHART_GROUP: 'a02',
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'MB'},
                            'Number of bytes eligible to be read from Col Cache(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rcchitby',
                              TITLE:       'CC Reads (Bytes)',
                              CHART_GROUP: 'a02',
                              CHECK_ZERO:  True,
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'MB' },
                            'Number of bytes of keep read hits from Col cache(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rcckphitby',
                              TITLE:       'CC Keep Reads (Bytes)',
                              CHART_GROUP: 'a02',
                              CHECK_ZERO:  True,
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'MB' },
                            'Number of bytes saved by hits from Columnar Cache(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rccsaveby',
                              TITLE:       'CC Saved (Bytes)',
                              CHART_GROUP: 'a02',
                              CHECK_ZERO:  True,
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'MB' },
                            'Total size for nocache read(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'rnocacheby',
                              TITLE:       'No Cache Reads (Bytes)',
                              CHART_GROUP: 'a02',
                              CHECK_ZERO:   True,
                              STAT_UNIT:    'KB',
                              DISP_UNIT:    'MB'},
                            # Flash cache writes
                            'Number of cache writes':
                            { METRIC_TYPE: METRIC_DELTA,
                              KEY:          'wr',
                              TITLE:       'Writes',
                              CHART_GROUP: 'a03',
                              CHECK_ZERO:   True},
                            'Number of keep cache writes':
                            { METRIC_TYPE: METRIC_DELTA,
                              KEY:          'wrkp',
                              TITLE:       'Keep Writes',
                              CHART_GROUP: 'a03',
                              CHECK_ZERO:   True},
                            'Number of partial cache writes':
                            { METRIC_TYPE: METRIC_DELTA,
                              KEY:         'pcwr',
                              TITLE:       'Partial Writes',
                              CHART_GROUP: 'a03',
                              CHECK_ZERO:   True},
                            'Number of redirty':
                            { METRIC_TYPE: METRIC_DELTA,
                              KEY:         'redirty',
                              TITLE:       'Redirty',
                              CHART_GROUP: 'a03',
                              CHECK_ZERO:   True},
                            'Number of nocache writes':
                            { METRIC_TYPE: METRIC_DELTA,
                              KEY:         'nocachewr',
                              TITLE:       'No Cache Writes',
                              CHART_GROUP: 'a03',
                              CHECK_ZERO:   True},
                            # flash cache write byes
                            'Total size for cache writes(KB)':
                            { METRIC_TYPE: METRIC_DELTA,
                              KEY:          'wrby',
                              TITLE:       'Writes (Bytes)',
                              CHART_GROUP: 'a04',
                              CHECK_ZERO:   True,
                              STAT_UNIT:    'KB',
                              DISP_UNIT:    'MB'},
                            'Total size for keep cache writes(KB)':
                            { METRIC_TYPE: METRIC_DELTA,
                              KEY:          'wrkpby',
                              TITLE:       'Keep Writes (Bytes)',
                              CHART_GROUP: 'a04',
                              CHECK_ZERO:   True,
                              STAT_UNIT:    'KB',
                              DISP_UNIT:    'MB'},
                            'Total size for nocache writes(KB)':
                            { METRIC_TYPE: METRIC_DELTA,
                              KEY:         'nocachewrby',
                              TITLE:       'No Cache Writes (Bytes)',
                              CHART_GROUP: 'a04',
                              CHECK_ZERO:   True,
                              STAT_UNIT:    'KB',
                              DISP_UNIT:    'MB'},
                            # Flash cache size
                            'Cachesize(KB)':
                            { METRIC_TYPE: METRIC_CURRENT,
                              KEY:          'sz',
                              CHART_GROUP: 'a07',
                              CHECK_ZERO:   True,
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'GB'},
                            'Keepsize(KB):':
                            { METRIC_TYPE: METRIC_CURRENT,
                              KEY:          'kpsz',
                              CHART_GROUP: 'a07',
                              CHECK_ZERO:   True,
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'GB'},
                            'OLTPsize(KB):':
                            { METRIC_TYPE: METRIC_CURRENT,
                              KEY:          'olsz',
                              CHART_GROUP: 'a07',
                              CHECK_ZERO:   True,
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'GB'},
                            'Columnar Cache used size (KB)':
                            { METRIC_TYPE: METRIC_CURRENT,
                              KEY:          'ccusedsz',
                              CHART_GROUP: 'a07',
                              CHECK_ZERO:   True,
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'GB'},
                            'Columnar Cache keep Size (KB)':
                            { METRIC_TYPE: METRIC_CURRENT,
                              KEY:          'cckpsz',
                              CHART_GROUP: 'a07',
                              CHECK_ZERO:   True,
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'GB'},
                            # internal IO
                            'Number of disk writer writes' :
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'dkwr',
                              CHECK_ZERO:  True,
                              TITLE:       'Disk Writer Writes',
                              CHART_GROUP: 'a05'},
                            'Number of disk writer chunk reads':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'dkwrr',
                              CHECK_ZERO:  True,
                              TITLE:       'Disk Writer Chunk Reads',
                              CHART_GROUP: 'a05'},
                            'Number of disk writer chunk writes':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'dkwrw',
                              CHECK_ZERO:  True,
                              TITLE:       'Disk Writer Chunk Writes',
                              CHART_GROUP: 'a05'},
                            'Number of populates':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'popwr',
                              TITLE:       'Populates',
                              CHECK_ZERO:  True,
                              CHART_GROUP: 'a05'},
                            # internal IO bytes
                            'Total size for disk writer writes(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'dkwrby',
                              CHECK_ZERO:  True,
                              TITLE:       'Disk Writer Writes (Bytes)',
                              CHART_GROUP: 'a06',
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'MB'},
                            'Total size for disk writer chunk reads(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'dkwrrby',
                              CHECK_ZERO:  True,
                              TITLE:       'Disk Writer Chunk Reads (Bytes)',
                              CHART_GROUP: 'a06',
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'MB'},
                            'Total size for disk writer chunk writes(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'dkwrwby',
                              CHECK_ZERO:  True,
                              TITLE:       'Disk Writer Chunk Writes (Bytes)',
                              CHART_GROUP: 'a06',
                              STAT_UNIT:   'KB',
                              DISP_UNIT:   'MB'},
                            'Total size for populate writes(KB)':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'popwrby',
                              TITLE:       'Populates (Bytes)',
                              CHECK_ZERO:  True,
                              CHART_GROUP: 'a06',
                              STAT_UNIT:    'KB',
                              DISP_UNIT:    'MB'},
                            # flash cache failed attempts
                            'Number of failed attempts to get a cacheline from lru':
                            { METRIC_TYPE: METRIC_DELTA ,
                              KEY:         'fllru',
                              CHECK_ZERO:  True,
                              TITLE:       'Failed attempts to get cacheline from lru'},
                           },
                       },
            GROUP_FFI: { KEY: 'ffi', TITLE: 'FFI',
              METRIC_LIST: {}
                       },
            GROUP_BIO: { KEY: 'bio', TITLE: 'Block I/O',
              METRIC_LIST: {}
                       }
           }

GROUP_MAX_LEN = max(len(x) for x in METRIC_METADATA)


#------------------------------------------------------------
def _get_exa_interval(sample_interval_line):
  '''
    returns the interval used in the cellsrvstat command as seen in
    the header file
  '''
  try:
    # if parsing actual command
    # interval = command.split()[4].rsplit('=',1)[1]
    # retrieve from sample interval in header
    interval = sample_interval_line.rsplit(None,1)[1]
  except Exception as e:
    _my_report_context.log_msg('warning', 'Using default interval for cellsrvstat')
    interval = 5
  finally:
    return int(interval)

#------------------------------------------------------------
def _update_bucket(bucket,
                   group_name,
                   metric_name,
                   delta_value,
                   current_value,
                   check_zero,
                   summary_stats,
                   exa_interval = 5):

  '''
    updates the buckets with the information for this metric
    PARAMETERS:
      bucket       : bucket in buckets to update
      group_name   : group that we are currently processing
      metric_metadata: metric metadata for this group
                       this is from METRIC_METADATA
      delta_value  : delta value column from the file
      current_value: current value column from the file
      check_zero   : metrics to keep track if 0 for this host
      exa_interval : interval used in the cellsrvstat file,
                     to compute per second rates if needed
      summary_stats: running total for entire interval
  '''
  metric_metadata = METRIC_METADATA[group_name][METRIC_LIST][metric_name]
  
  # generate a key to key into the bucket, using group and metric
  # gname = [ k for k in METRIC_METADATA if k == group_name ][0]
  key = generate_key(METRIC_METADATA[group_name][KEY], metric_metadata[KEY])

  # get metadata for metric - to determine if we want delta or current,
  # and add to the check_zero list if needed
  if metric_metadata[METRIC_TYPE] == METRIC_DELTA:
    # convert delta values to per second rates
    v = float(delta_value)/exa_interval
    # also maintain running total if we need to check if stat value is 0
    if key in check_zero and delta_value > 0:
      check_zero[key] += long(delta_value)

  elif metric_metadata[METRIC_TYPE] == METRIC_CURRENT:
    v = long(current_value)
    if key in check_zero and current_value > 0:
      check_zero[key] += long(current_value)

  # convert units
  # TODO: change this if more unit conversions are required
  if STAT_UNIT in metric_metadata and DISP_UNIT in metric_metadata and metric_metadata[STAT_UNIT] != metric_metadata[DISP_UNIT]:
    stat_unit = metric_metadata[STAT_UNIT]
    disp_unit = metric_metadata[DISP_UNIT]
    if stat_unit == 'KB' and disp_unit == 'GB':
      v = float(v)/1024/1024
    elif stat_unit == 'KB' and disp_unit == 'MB':
      v = float(v)/1024

  # now add the value to the bucket, initializing bucket if needed
  if key not in bucket:
    bucket[key] = { VALUE: 0, CNT: 0 }
  bucket[key][VALUE] += v;
  bucket[key][CNT] += 1

  # and add to summary too
  if key not in summary_stats:
    summary_stats[key] = { VALUE: 0, CNT: 0 }
  summary_stats[key][VALUE] += v;
  summary_stats[key][CNT] += 1;


#------------------------------------------------------------
def generate_key(gkey,mkey):
  '''
    generates a concatenated key based on the gkey, mkey
  '''
  return gkey + '_' + mkey

#------------------------------------------------------------
def _process_rules(report_context):

  # list of callbacks for rule processing
  # note, this structure is different from other modules, as
  # we want to specify keys in the declaration of the rules
  # given the number of metrics in cellsrvstat
  # processing the keys will be up to the individual rules
  RULES_CELLSRVSTAT = [
    { 'callback' : exawrules.rule_cellsrvstat_01_mem_failures,
      'keys'     : [ ( 'mem', 'fail512' ),
                     ( 'mem', 'fail2K' ),
                     ( 'mem', 'fail4K' ),
                     ( 'mem', 'fail8K' ),
                     ( 'mem', 'fail16K' ),
                     ( 'mem', 'fail32K' ),
                     ( 'mem', 'fail64K' ),
                     ( 'mem', 'fail1M' ) ] },
    { 'callback' : exawrules.rule_cellsrvstat_02_sio_pt,
      'keys' : [ ('sio', 'elig'),
                 ('sio', 'cpu' ),
                 ('sio', 'pt') ] },
    { 'callback' : exawrules.rule_cellsrvstat_03_fc_oltp_hit,
      'keys'     : [ ('fc', 'rhit' ),
                     ('fc', 'rmiss') ] }
    ]


  for host in sorted(report_context.hostnames):
    # ignore multi-cell information
    if host == '':
      continue

    for rule in RULES_CELLSRVSTAT:
      # generate the tuple with the additional information
      # we have the list of generated keys here, so that the
      # rules know exactly which ones they should be looking at
      info = ()
      for key in rule['keys']:
        info += ( generate_key(key[0], key[1]), )

      rule['callback'](report_context.hostnames[host].cellsrvstat, info)

    report_context.log_msg('debug','cellsrvstat findings: %s' % str(report_context.hostnames[host].cellsrvstat.findings))
#------------------------------------------------------------
def parse_input_files(filelist, report_context):

  '''
    This is the main routine in this module, which parses the files
    and populates the buckets

    PARAMETERS:
      filelist  : list of files to process, can be bz2, gz or text
      report_context: report context with start/end times and bucket
                      information
    DESCRIPTION:
      This will set the following global variables
        buckets - dictionary object keyed by bucket_id with datapoints
      In HostMetadataCellSrvStat object:
        processed_files - list of files processed
        check_zero - running total of metrics which we check if all zero
        metric_keys - metric_keys (with metric_metadata) we saw in the file

    As we parse the file, the datapoints are accumulated in each bucket
    After parsing, we go through a second pass to compute the average
    within each bucket.  (Note: we do this so that after parsing,
    any module - i.e. using gnuplot or google charts, can simply
    plot the data without having to calculate averages)

    We also maintain a list of processed_start_times - this is based on the
    'Starting Time' string at the start of the exawatcher cellsrvstat file.
    If we see the same 'Starting Time' (for same host) we skip the file
    and move onto the next file

  '''
  global buckets
  global hostnames
  global _my_report_context
  _my_report_context = report_context

  state = None
  metrics = {}

  # list of file start times we have processed, based on header in file
  processed_start_times = []

  # go through list of files
  for fname in (filelist):
    # determine filetype

    try:
      ftype = file_type(fname, _my_report_context)
      input_file = open_file(fname, ftype)
      if ftype == FILE_UNKNOWN or input_file == None:
        raise UnrecognizedFile(fname + '(' + ftype + ')')

      hostname = get_hostname_from_filename(fname)

      # first check file header to ensure this is ExaWatcher cellsrvstat file
      header = [next(input_file) for x in xrange(EXAWATCHER_HEADER_LINES)]

      # we expect the module to be in the 4th line
      if EXAWATCHER_CELLSRVSTAT_MODULE_NAME not in header[EXAWATCHER_MODULE_POSITION]:
        raise UnrecognizedFile(fname)

      # check if we have processed this file based on start time
      if (hostname,header[EXAWATCHER_STARTING_TIME_POSITION]) in processed_start_times:
        raise DuplicateFile(fname)

      # extract Starting Time from ExaWatcher header, and get last two
      # strings after split()
      (file_start_date_str,file_start_time_str) = header[EXAWATCHER_STARTING_TIME_POSITION].strip().split()[-2:]
      # construct datetime object of the file start time
      file_start_time = datetime.strptime(file_start_date_str + ' ' +
                                          file_start_time_str,
                                          DATE_FMT_INPUT)

      file_end_time = get_file_end_time(file_start_time, header[EXAWATCHER_SAMPLE_INTERVAL_POSITION], header[EXAWATCHER_ARCHIVE_COUNT_POSITION])

      # check if we have data in the file for our report interval
      if file_end_time < _my_report_context.report_start_time or file_start_time > _my_report_context.report_end_time:
        raise NoDataInFile(fname)

    except UnrecognizedFile as e:
      _my_report_context.log_msg('warning', 'Unrecognized file: %s' % (e.value))
    except DuplicateFile as e:
      _my_report_context.log_msg('warning', 'Ignoring duplicate file: %s' % (e.value))
    except NoDataInFile as e:
      _my_report_context.log_msg('warning', 'No data within report interval in file: %s' % (e.value))
    except IOError as e:
      if e.errno == errno.EACCES:
        _my_report_context.log_msg('error', 'No permissions to read file: %s (%s)' % (fname, str(e)))
      else:
        _my_report_context.log_msg('error', 'Unable to process file: %s: %s' % (fname, str(e)))
      
    except Exception as e:
      _my_report_context.log_msg('error', 'Unable to process file: %s:%s' % (fname, str(e)))

    else:
      # only add if we will be processing the file
      if hostname not in hostnames:
        hostnames[hostname] = HostMetadataCellSrvStat(hostname)

      _my_report_context.add_hostinfo(hostname)
      
      # otherwise include in list and continue processing
      processed_start_times.append( (hostname,header[EXAWATCHER_STARTING_TIME_POSITION]) )
      hostnames[hostname].processed_files.append(fname)

      # get exawatcher interval for this file, to compute per second rates
      exa_interval = _get_exa_interval(header[EXAWATCHER_SAMPLE_INTERVAL_POSITION])

      # initialize bucket_id
      bucket_id = -1

      # go through file
      for line in input_file:
        line = line.rstrip()  # remove newline
        tokens = line.split() # split into tokens

        # skip blank lines
        if len(line) == 0:
          continue

        # check if this has the timestamp
        if GROUP_TS in line:
          state = GROUP_TS
          # contruct the timestamp
          line = line.replace(GROUP_TS,'').strip()
          # note: we expect format to be "Day Mon DD hh:mi:ss YYYY"
          sample_time = datetime.strptime(line,'%a %b %d %H:%M:%S %Y')

          # for samples in our desired range, get the bucket_id
          if sample_time >= _my_report_context.report_start_time and sample_time <= report_context.report_end_time:
              bucket_id = _my_report_context.get_bucket_id(sample_time)
              # add the timestamp of the bucket, not the sample time
              # as many samples can fall into a bucket
              if bucket_id not in buckets:
                buckets[bucket_id] = { hostname : {} }
          else:
            bucket_id = -1

        # if we recognize this group - get the metric metadata
        elif line in METRIC_METADATA:
          # fortify get key from our list, rather than from line
          state = [k for k in METRIC_METADATA if k in line][0]
          metrics = METRIC_METADATA[state][METRIC_LIST]

        # if we have a valid bucket (i.e in desired time range)
        # and this is a group we are interested in (based on state)
        # and this has the metric that we want (based on metrics)
        # (we remove last two columns to determine the metric name),
        # then we process it into our bucket
        elif bucket_id != -1 and state in METRIC_METADATA and line.rsplit(None,2)[0] in metrics:
          # get delta and current values - based on last two columns in line
          (mname, delta, current) = line.rsplit(None,2)
          # get metadata for the metric
          # fortify, redirect rather than using file input directly
          metric_name = [ m for m in metrics if m == mname ][0]
          # metric_metadata = metrics[metric_name]
          # and update the bucket
          if hostname not in buckets[bucket_id]:
            buckets[bucket_id][hostname] = {}
          _update_bucket(buckets[bucket_id][hostname],
                         state, metric_name, delta, current,
                         hostnames[hostname].check_zero,
                         _my_report_context.hostnames[hostname].cellsrvstat.summary_stats,
                         exa_interval)


    finally:
      if input_file != None:
        input_file.close()

  # now get averages within each bucket so clients can consume data directly
  # but maintain a list of keys that we actually saw (to allow charting to
  # work with older cell versions that may not have all the metrics),
  # so we know which charts are valid, rather than using metadata only
  if len(hostnames) > 1:
    _my_report_context.set_multihost(True)

  for i in buckets:
    for host in buckets[i]:
      for key in buckets[i][host]:
        if key not in hostnames[host].metric_keys:
          hostnames[host].metric_keys.append(key)
        data_bucket = buckets[i][host][key]
        v = data_bucket[VALUE]
        cnt = data_bucket[CNT]
        if cnt != 0:
          data_bucket[VALUE] = v/cnt

  # also maintain summary stats
  for host in _my_report_context.hostnames:
    cs_summary = _my_report_context.hostnames[host].cellsrvstat.summary_stats
    for key in cs_summary:
      if cs_summary[key][CNT] != 0:
        cs_summary[key][VALUE] = cs_summary[key][VALUE]/cs_summary[key][CNT]

  _process_rules(_my_report_context)

#------------------------------------------------------------
def main():
  global _my_report_context
  _my_report_context = ReportContext()
  _my_report_context.log_msg('error', 'exaparse main noop')
#
#------------------------------------------------------------
# standard template
#------------------------------------------------------------
if __name__ == '__main__':
  main()







