#!/usr/bin/python
#
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
#
#     NAME
#       exaparse_mp.py
#
#     DESCRIPTION
#       Parses ExaWatcher mpstat data and produces bucketes with the data
#       along with a summary that aggregates data per cpu
#
#     MODIFIED   (MM/DD/YY)
#     cgervasi    12/01/16 - handle all 0 values for cpu
#     cgervasi    09/28/16 - add summary page
#     cgervasi    09/14/16 - Creation
#
#     NOTES:
#       If the format of the mpstat file changes, this could potentially
#       break.
#
#       We expect
#       . first few lines of file to conform to ExaWatcher format
#       . since timestamp is not printed before each sample, we need
#         to calculate it ourselves based on interval

import re

from datetime import datetime,timedelta
from glob import glob

from exawutil import DEFAULT_MAX_BUCKETS, TIMESTAMP, CNT, CPU, USR, NICE, SYS, WIO, STL, IDL, BUSY, EXAWATCHER_STARTING_TIME_POSITION, EXAWATCHER_SAMPLE_INTERVAL_POSITION, EXAWATCHER_ARCHIVE_COUNT_POSITION, EXAWATCHER_MODULE_POSITION, EXAWATCHER_COLLECTION_COMMAND_POSITION, EXAWATCHER_MISC_INFO_POSITION, EXAWATCHER_HEADER_LINES, DATE_FMT_INPUT, FILE_UNKNOWN, file_type, open_file, get_file_end_time, get_hostname_from_filename, UnrecognizedFile, DuplicateFile, NoDataInFile, HostNameMismatch, ReportContext, HostMetadata

import exawrules

IRQ = 'irq'
SOFT = 'soft'
GUEST= 'guest'

IDLE_THRESHOLD_ALL_CPUS=20  # if average CPU usage is low 
IDLE_THRESHOLD_MAX_CPUS=15  # but a cpu has higher utilization
USER_THRESHOLD_MAX_CPUS=10  # and it has low %usr ...


#------------------------------------------------------------
# For pasing the file we need to group into buckets
# note, we do not display multi-cell information for mpstat, but
# we need to be able to parse data if it comes from multiple cells
#
# bucket:
#   <bucket_id>: { <hostname>:
#     { <cpu_id|all>: { USR: <x>, NICE: <x>, SYS: <x>,
#                       WIO: <x>, IRQ: <x>, SOFT: <x>,
#                       STL: <x>, GUEST: <x>, IDL:<x>,
#                       BUSY: <x>, CNT: <x> }
#
# note, since summary is per cpu, we need
# it should have the same structure as a bucket ...
#

#------------------------------------------------------------
# Globals - initialize
buckets = {}
hostnames= {}  # objects keyed by hostname to HostMetadataMpstat object

# private variable for report context
# this is created by caller of parse_input_files
_my_report_context = None

EXAWATCHER_MPSTAT_MODULE_NAME = 'MpstatExaWatcher'

#------------------------------------------------------------
# extend HostMetadata to include information about potential
# high cpu usage on a subset of cpus
class HostMetadataMpstat(HostMetadata):
  def __init__(self,hostname):
    super(HostMetadataMpstat,self).__init__(hostname)
    self.flag_warning = []  # orange flag, high cpu usage
    self.flag_alert   = []  # red flag, high cpu usage, low %usr

  def __str__(self):
    str = super(HostMetadataMpstat,self).__str__()
    return str + ', flag_warning: %s, flag_alert: %s' % (self.flag_warning,
                                                         self.flag_alert)

#------------------------------------------------------------
def _init_cpustat():
  return { USR: 0, NICE: 0, SYS: 0, WIO: 0,
           IRQ: 0, SOFT: 0, STL: 0, GUEST: 0,
           IDL: 0, BUSY: 0, CNT: 0 }

#------------------------------------------------------------
def _parse_time_format(tokens,base_date_str,file_start_time):
  '''
    Parses the sample time in the ExaWatcher mpstat file and returns
    a datetime object constructed from the sample time seen in line

    This understands two formats
    hh:mi:ss <AM|PM>
    hh24:mi:ss

    In the first case, it converts AM|PM to a 24 hour-clock then constructs
    the full timestamp by adding base_date_str to get the date component

    In both cases, it looks for wrapping over midnight, so that the
    date can be modified by a day
  '''
  # get the time - always first token
  time_str = tokens[0]
  wrap = False
  has_ampm = False
  # check if time format is hh:mi:ss AM|PM
  if tokens[1] == 'AM' or tokens[1] == 'PM':
    has_ampm = True
    time_str += ' ' + tokens[1]  # add AM|PM to time string
    # check if we wrapped midnight in file based on AM/PM
    time_str_am_pm = time_str.rsplit(None,1)[1]
    start_time_am_pm = file_start_time.strftime('%p')
    if (start_time_am_pm == 'PM' and time_str_am_pm == 'AM'):
      wrap = True
      
  # time format is hh24:mi:ss
  else:
    sample_hh = time_str[0:2]
    file_hh = file_start_time.strftime('%H')

    if (sample_hh < file_hh):
      wrap = True

  if wrap:      
    # add a day to the base_date
    base_date = datetime.strptime(base_date_str, '%m/%d/%Y') + timedelta(days=1)
  else:
    # keep the same start date as the start of the file
    base_date = datetime.strptime(base_date_str, '%m/%d/%Y')
    
  # construct a new date string
  new_base_date_str = datetime.strftime(base_date,'%m/%d/%Y')
  
  # and convert to the a datetime object
  if has_ampm:
    sample_time = datetime.strptime(new_base_date_str + ' ' + time_str,
                                    '%m/%d/%Y %I:%M:%S %p')
  else:
    sample_time = datetime.strptime(new_base_date_str + ' ' + time_str,
                                    '%m/%d/%Y %H:%M:%S')
  return sample_time


#------------------------------------------------------------
def _update_cpu_bucket(bucket, usr, nice, sys, wio, irq, soft, steal, guest, idle):
  '''
    updates the stats for the cpu bucket
  '''
  bucket[USR]  += float(usr)
  bucket[NICE] += float(nice)
  bucket[SYS]  += float(sys)
  bucket[WIO]  += float(wio)
  bucket[IRQ]  += float(irq)
  bucket[SOFT] += float(soft)
  bucket[STL]  += float(steal)
  # we do not always have guest here ...
  if guest != None:
    bucket[GUEST]+= float(guest)
  else:
    bucket[GUEST] = None
  bucket[IDL]  += float(idle)
  bucket[CNT]  += 1
  
#------------------------------------------------------------
def _parse_cpu(tokens, bucket_id, hostname, stat_pos, summary):
  '''
    parses the cpu line with the actual metrics
    note that position is dynamically determined based on the header line
  '''
  cpu_id = tokens[stat_pos[CPU]]
  if cpu_id != 'all':
    cpu_id = int(cpu_id)  # use numeric for cpu id
  usr    = tokens[stat_pos[USR]]
  nice   = tokens[stat_pos[NICE]]
  sys    = tokens[stat_pos[SYS]]
  wio    = tokens[stat_pos[WIO]]
  irq    = tokens[stat_pos[IRQ]]
  soft   = tokens[stat_pos[SOFT]]
  steal  = tokens[stat_pos[STL]]
  guest = None 
  if stat_pos[GUEST] != None:
    guest  = tokens[stat_pos[GUEST]]
  idle   = tokens[stat_pos[IDL]]

  # bucket for the host should already have been created here
  # we do not check ...
  bucket = buckets[bucket_id][hostname]
  if cpu_id not in bucket:
    bucket[cpu_id] = _init_cpustat()
  if cpu_id not in summary:
    summary[cpu_id] = _init_cpustat()

  # update stats for the bucket
  _update_cpu_bucket(bucket[cpu_id], usr, nice, sys, wio, irq, soft, steal, guest, idle)

  # update stats for the summary
  _update_cpu_bucket(summary[cpu_id], usr, nice, sys, wio, irq, soft, steal, guest, idle)


#------------------------------------------------------------
def _flag_cpus(report_context):
  '''
    To find out if we have a problem (from Kodi):
    . see if a subset of cpus have low %idle (orange flag)
    . if same subset has low %usr (red flag)
    Note: this has to be per host
  '''

  # basically, we check
  # . if avergae cpu utilization is low (%idle >= IDLE_THRESHOLD_ALL_CPUs)
  #   which means %utilization is <= 80%
  # . check if 1 or more cpus has high utilization
  #   (%idle <= IDLE_THRESHOLD_MAX_CPUS)
  #   which means low idle, high utilization
  # . and then check if those same CPUs have low %user
  #   %usr < USER_THRESHOLD_MAX_CPUS

  # process for all hosts
  for host in report_context.hostnames:
    # ignore multihost entry, it won't have summaries for flagging
    if report_context.multihost and host == '':
      continue

    # get 'all' for comparison purposes  
    all_cpu = report_context.hostnames[host].mpstat.summary_stats['all']
    num_cpus = len(report_context.hostnames[host].mpstat.summary_stats) - 1 # remove 'all'
    
    # busy system, do not bother checking for outliers
    if all_cpu[IDL] < IDLE_THRESHOLD_ALL_CPUS:
      continue
    # otherwise, look for potential individual cpus that are maxed out
    mpstat_summary = report_context.hostnames[host].mpstat.summary_stats
    for cpu_id in mpstat_summary:
      # customer bug25102232: only check for high cpu if busy != 0
      if cpu_id != 'all' and mpstat_summary[cpu_id][BUSY] != 0:
        # check if low %idle, and low %user, then this is a red flag
        if mpstat_summary[cpu_id][IDL] <= IDLE_THRESHOLD_MAX_CPUS and mpstat_summary[cpu_id][USR] <= USER_THRESHOLD_MAX_CPUS:
          hostnames[host].flag_alert.append(cpu_id)
        # low % idle, but %user is high 
        elif mpstat_summary[cpu_id][IDL] <= IDLE_THRESHOLD_MAX_CPUS:
          hostnames[host].flag_warning.append(cpu_id)

#------------------------------------------------------------
def parse_input_files(filelist,
                      report_context):
  '''
    This is the main routine in this module, which parses the
    files and populates buckets and summary

    PARAMETERS:
      filelist: list of files to process, can be bz2, gz or text
      report_context: report context with start/end times and bucket
                      information

    DESCRIPTION:
      This will set the following global variables
        buckets - dictionary object keyed by bucket_id with datapoints
        hostnames: where each metadata object includes
          hostname, processed files
        summary - same structure as one bucket, but includes summary
          information so we can aggregate all information per cpu id

      As we parse the file, the datapoints are accumulated in each bucket.
      After parsing, we go through a second pass to compute the average
      within each bucket.

      We also maintain a list of processed_start_times - this is based on
      the hostname and 'Starting Time' string at the start of the
      exawatcher mpstat file.
      If we see the same 'Starting Time' for the same host, we skip the
      file and move onto the next file

      We also check if the file has data for the timeframe of interest, if
      not, we skip the file
    
  '''
  # global variables set by this routine
  global buckets
  global summary
  global hostnames

  # list of file start times we have processed based on header
  processed_start_times = []

  # now go through list of files
  for fname in (filelist):
    try:
      # determine type of file, only process if we recognize the filetype
      ftype  = file_type(fname, report_context)
      input_file = open_file(fname, ftype)
      if ftype == FILE_UNKNOWN or input_file == None:
        raise UnrecognizedFile(fname + '(' + ftype + ')')

      # get hostname
      hostname = get_hostname_from_filename(fname)

      # first check file header to ensure this is ExaWatcher mpstat file
      header = [next(input_file) for x in xrange(EXAWATCHER_HEADER_LINES)]

      # we expect the module to the be the 4th line
      if EXAWATCHER_MPSTAT_MODULE_NAME not in header[EXAWATCHER_MODULE_POSITION]:
        # skip this file
        raise UnrecognizedFile(fname)

      # check if we have processed thie file based on start time
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
      if file_end_time < report_context.report_start_time or file_start_time > report_context.report_end_time:
        raise NoDataInFile(fname)

    except UnrecognizedFile as e:
      report_context.log_msg('warning', 'Unrecognized file: %s' % (e.value))
    except DuplicateFile as e:
      report_context.log_msg('warning', 'Ignoring duplicate file: %s' % (e.value))
    except NoDataInFile as e:
      report_context.log_msg('warning', 'No data within report interval in file: %s' % (e.value))
    except IOError as e:
      if e.errno == errno.EACCES:
        report_context.log_msg('error', 'No permissions to read file: %s (%s)' % (fname, str(e)))
      else:
        report_context.log_msg('error', 'Unable to process file: %s: %s' % (fname, str(e)))
      
    except Exception as e:
      report_context.log_msg('error', 'Unable to process file: %s:%s' % (fname, str(e)))

    else:
      # keep track of hosts we're processing
      if hostname not in hostnames:
        hostnames[hostname] = HostMetadataMpstat(hostname)

      report_context.add_hostinfo(hostname)
      
      processed_start_times.append( (hostname, header[EXAWATCHER_STARTING_TIME_POSITION]) )
      hostnames[hostname].processed_files.append(fname)

        
      # reset position of stats for each file
      get_stat_pos = True
      # default positions 
      stat_pos = { CPU: None,
                   USR: None , NICE: None , SYS: None , WIO: None ,
                   IRQ: None , SOFT: None , STL: None , GUEST: None ,
                   IDL: None }

      # initialize bucket
      bucket_id = -1

      for line in input_file:
        line = line.rstrip()
        tokens = line.split()

        # skip blank lines
        if len(tokens) == 0:
          continue

        # this is the header line if it contains CPU and has a timestamp as
        # the first token
        if get_stat_pos and 'CPU' in line and re.match('\d{2}:\d{2}:\d{2}',tokens[0]):
          try:
            stat_pos[CPU] = tokens.index('CPU')
            # user can be %usr or %user -- really!
            if '%user' in line:
              stat_pos[USR]  = tokens.index('%user')
            else:
              stat_pos[USR] = tokens.index('%usr')
            stat_pos[NICE] = tokens.index('%nice')
            stat_pos[SYS]  = tokens.index('%sys')
            stat_pos[WIO]  = tokens.index('%iowait')
            stat_pos[IRQ]  = tokens.index('%irq')
            stat_pos[SOFT]  = tokens.index('%soft')
            stat_pos[STL]   = tokens.index('%steal')
            # guest isn't always present
            if '%guest' in line:
              stat_pos[GUEST] = tokens.index('%guest')
            stat_pos[IDL]  = tokens.index('%idle')
            get_stat_pos = False
          except ValueError as e:
            report_context.log_msg('error','Unable to parse mpstat for file %s (%s)' % (fname,str(e)))
            raise

        # if we have the actual data, and we already know the position of
        # CPU
        elif stat_pos[CPU] != None and tokens[stat_pos[CPU]] != "CPU" and re.match('\d{2}:\d{2}:\d{2}',tokens[0]):
          # parse the time format, we need to get the date into it
          sample_time = _parse_time_format(tokens, file_start_date_str, file_start_time)

          # check if this is in our time range
          if sample_time >= report_context.report_start_time and sample_time <= report_context.report_end_time:
            # note, each sample has its own timestamp for mpstat
            bucket_id = report_context.get_bucket_id(sample_time)
            if bucket_id not in buckets:
              buckets[bucket_id] = {}
            if hostname not in buckets[bucket_id]:
              buckets[bucket_id][hostname] = {}

            _parse_cpu(tokens, bucket_id, hostname, stat_pos, report_context.hostnames[hostname].mpstat.summary_stats)
            
    finally:
      if input_file != None:
        input_file.close()

  # check for multihost
  if len(hostnames) > 1:
    report_context.set_multihost(True)
    
  # post-process buckets to compute true average
  for (i, bucket) in buckets.iteritems():
    for host in bucket:
      for cpu_id in bucket[host]:
        # compute stats for bucket
        bucket_cpu = bucket[host][cpu_id]
        if bucket_cpu[CNT] > 1:
          for stat in [ USR, NICE, SYS, WIO, IRQ, SOFT, STL, GUEST, IDL ]:
            if bucket_cpu[stat] != None:
              bucket_cpu[stat] = bucket_cpu[stat]/bucket_cpu[CNT]
          # busy is 100-idle
          bucket_cpu[BUSY] = 100 - bucket_cpu[IDL]

  # calculate summary too
  for host in report_context.hostnames:
    summary_stats = report_context.hostnames[host].mpstat.summary_stats
    for cpu_id in summary_stats:
      if summary_stats[cpu_id][CNT] > 1:
        cpu_total = 0
        for stat in [ USR, NICE, SYS, WIO, IRQ, SOFT, STL, GUEST, IDL ]:
          if summary_stats[cpu_id][stat] != None:
            summary_stats[cpu_id][stat] = summary_stats[cpu_id][stat]/summary_stats[cpu_id][CNT]
            cpu_total += summary_stats[cpu_id][stat]
        # customer bug25102232: check if all values are 0
        if cpu_total > 0:
          summary_stats[cpu_id][BUSY] = 100 - summary_stats[cpu_id][IDL]
        else:
          summary_stats[cpu_id][BUSY] = 0

  # now try and find out if we have maxed out some cpus
  _flag_cpus(report_context)
  
  _process_rules(report_context)

#------------------------------------------------------------
def _process_rules(report_context):
  # list of callbacks for rules
  RULES_MPSTAT=[ exawrules.rule_mpstat_01_high_cpu,
                 exawrules.rule_mpstat_02_high_cpu_subset_alert ,
                 exawrules.rule_mpstat_03_high_cpu_subset_warning  ]

  for host in report_context.hostnames:
    # skip multi-cell information
    if host == '':
      continue
    for rule in RULES_MPSTAT:
      # execute the callback
      # also pass in a tuple of additional information
      # individual rules should know how to read the tuple
      if rule == exawrules.rule_mpstat_01_high_cpu:
        info = ( IDLE_THRESHOLD_ALL_CPUS,  )
      else:  
        info = ( hostnames[host],  # host metadata
                 # number of cpus
                 len(report_context.hostnames[host].mpstat.summary_stats) - 1)
      rule(report_context.hostnames[host].mpstat,info)

    report_context.log_msg('debug', '%s findings: %s' %
                           (host,
                            report_context.hostnames[host].mpstat.findings))

    
  
#------------------------------------------------------------
def main():
  # for unit test, hard code files ..
  _my_report_context = ReportContext()
  _my_report_context.set_log_level('DEBUG')
  
  filelist_tmp = re.sub(r'\s', ' ', '/scratch/cgervasi/esc/sr3-13005885771/0803/Searched_2016*/Mpstat.ExaWatcher/*').split(' ')
  filelist = []
  for f in filelist_tmp:
    filelist += glob(f)
  _my_report_context.log_msg('debug','%s' % filelist)
  _my_report_context.set_report_context(start_time = datetime.strptime('08/03/2016 13:30:00',DATE_FMT_INPUT),
                                        end_time = datetime.strptime('08/03/2016 14:30:00',DATE_FMT_INPUT),
                                        max_buckets = 5,
                                        outdir = '.')
                                        
  parse_input_files(filelist, _my_report_context)

#
#------------------------------------------------------------
# standard template
#------------------------------------------------------------
if __name__ == '__main__':
  main()
