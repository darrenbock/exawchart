#!/usr/bin/python

#
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
#
#     NAME
#       exaparse.py
#
#     DESCRIPTION
#       Parses ExaWatcher iostat data and produces bucketes with the data
#
#     MODIFIED   (MM/DD/YY)
#     cgervasi    11/07/16 - fix summary
#     cgervasi    09/28/16 - add summary page
#     cgervasi    06/21/16 - fortify
#     cgervasi    05/16/16 - add support for multiple hosts
#     cgervasi    03/24/16 - Creation
#
#     NOTES:
#       If the format of the iostat file changes, this could potentially
#       break.
#
#       We expect
#       . first few lines of file to conform to ExaWatcher format
#       . timestamp to be printed before each iostat sample
#         This currently parses two different time formats for the sample
#         . Time: hh:mi:ss <AM|PM>
#         . mm/dd/yy hh24:mi:ss
#         If the format changes, this will need to be updated; if there's
#         globalization in the way the time formats are displayed, this
#         will break

import re
import errno
from datetime import datetime, timedelta
import distutils.spawn
from operator import itemgetter
from subprocess import Popen, PIPE
from lxml import etree

 
# import constants and common functions from exaioutil
from exawutil import DATE_FMT_INPUT, TIMESTAMP, CPU, FLASH, DISK, CNT, USR, NICE, SYS, WIO, STL, IDL, BUSY, RPS, WPS, RSECPS, WSECPS, AVGRQSZ, AVGQUSZ, AWAIT, SVCTM, UTIL, RMBPS, WMBPS, IOPS, MBPS, SUMMARY, DEFAULT_FLASH_DISKS, DEFAULT_HARD_DISKS, EXAWATCHER_STARTING_TIME_POSITION, EXAWATCHER_SAMPLE_INTERVAL_POSITION, EXAWATCHER_ARCHIVE_COUNT_POSITION, EXAWATCHER_MODULE_POSITION, EXAWATCHER_COLLECTION_COMMAND_POSITION, EXAWATCHER_MISC_INFO_POSITION, EXAWATCHER_HEADER_LINES, FILE_UNKNOWN, FINDING_TYPE_INFO, file_type, open_file, get_file_end_time, get_hostname, get_hostname_from_filename, validate_disk, UnrecognizedFile, DuplicateFile, NoDataInFile, HostNameMismatch, ReportContext,HostMetadata

import exawrules

# ------------------------------------------------------------
# For parsing the file we need to group into buckets so we do not
# have too many datapoints.  Based on the time range to plot, we
# will compute the number of buckets (maximum of DEEFAULT_MAX_BUCKETS)
# and determine the bucket interval (bucket intervals are multiples of
# MIN_BUCKET_INTERVAL).  This is all set in ReportContext().
#
# Each sample that we read will then fall into a specified bucket
# As we read the file, we maintain the sums and counts in the
# buckets, so we can later calculate the average value for the
# bucket
#
# Structure of buckets (to handle a possibly different list of
# flash and hard disks in each ExaWatcher file):
# . buckets: dictionary object, keyed by bucket_id
#            in effect, an array where the bucket_id is the index
#            into the array (we don't use a python list in case
#            there are gaps in the timeline, then index into the list
#            will not be correct bucket_id)
#            Each bucket_id corresponds to a bucket dictionary object
#            (ie a struct)
# . bucket:
#     <bucket_id>: { <hostname>: {
#                      CPU: { USR: <x>, NICE: <x>, SYS: <x>, WIO: <x>,
#                             IDL: <x>, STL: <x>, CNT: <x> }
#                      FLASH: { <device>: {RPS: <x>,
#                                          WPS: <x>,
#                                          RSECPS: <x>,
#                                          WSECPS: <x>,
#                                          AVRQSZ: <x>,
#                                          AVQUSZ: <x>,
#                                          AWAIT:  <x>,
#                                          SVCTM:  <x>,
#                                          UTIL:  <x>,
#                                          CNT:  <x>}
#                               ... # multiple devices
#                      DISK: { <device>: {RPS: <x>,
#                                         WPS: <x>,
#                                         RSECPS: <x>,
#                                         WSECPS: <x>,
#                                         AVRQSZ: <x>,
#                                         AVQUSZ: <x>,
#                                         AWAIT:  <x>,
#                                         SVCTM:  <x>,
#                                         UTIL:  <x>,
#                                         CNT:  <x>}
#                               ... # multiple devices
#                               }
#                   }
# At first pass, the stats for CPU, FLASH and DISK contain SUMs for
# all the lines read, while CNT has the number of samples.
#
# On the second pass, we calculate the averages and store that in the
# bucket.
#
# We already separate out FLASH and DISKS within each bucket, as
# the disks could potentially change.  This also makes it easier to
# compute aggregates for FLASH and DISKS.
#
# Within FLASH and DISK, each entry is keyed by the device name.
#
# After the second pass we also have a SUMMARY dictionary object
# under FLASH/DISK which contains the aggregate information
# (sum for IOPs, MBPs, average for the other stats) for all flash/hard
# disks in that bucket. (FUTURE OPTIMIZATION if required: sort files,
# assume data is arriving in sorted order and can compute summary
# each time we change bucket_id)
#
# we also have an overall summary structure, which has the same
# structure as a bucket.  This is used for calculating the average
# over the entire time frame which we can then display in a summary
# page
#
# In order to support multiple hosts, we maintain the following per host
# . list of flash/hard disks, this is later used by the consumer of the
#   data, so we have the correct list of disks
# . list of processed files
#
# The output of this module is meant to be consumed by different
# scripts that can process the data so it can be consumed by 
# charting utilities such as gnuplot, google-charts or JET
#

#------------------------------------------------------------
# extend HostMetadata to include information for flash disks/hard disks
# for multi-host support
class HostMetadataIostat(HostMetadata):
  def __init__(self,hostname):
    super(HostMetadataIostat,self).__init__(hostname)
    self.flash_disks = []
    self.hard_disks = []
    self.capacity = None  # should only be populated if we are running on
                          # the host, can eventually be used for ref line
                          # note: format should be
                          # { FLASH: { IOPS: value, MBPS: value },
                          #   DISK:  { IOPS: value, MBPS: value } }

  def __str__(self):
    str = super(HostMetadataIostat,self).__str__()
    return str + ', flash_disks: %s, hard_disks: %s' % (self.flash_disks,
                                                        self.hard_disks)

  def get_cell_capacity(self, disktype, stattype):
    capacity = None
    if self.capacity != None:
      # we use the length of the disk list to determine cell capacity
      # note that if the list changes over time from different files
      # we could over calculate the cell capacity
      if disktype == FLASH and stattype == IOPS:
        capacity = len(self.flash_disks) * self.capacity[FLASH][IOPS]
      elif disktype == FLASH and stattype == MBPS:
        capacity = len(self.flash_disks) * self.capacity[FLASH][MBPS]
      elif disktype == DISK and stattype == IOPS:
        capacity = len(self.hard_disks) * self.capacity[DISK][IOPS]
      elif disktype == DISK and stattype == MBPS:
        capacity = len(self.hard_disks) * self.capacity[DISK][IOPS]
    return capacity
      

#------------------------------------------------------------
# Globals - initialize
buckets = {}
hostnames = {}  # object keyed by hostname to HostMetadataIostat objects

# private variable for report context
# this is created by the caller of parse_input_files
_my_report_context = None

EXAWATCHER_IOSTAT_MODULE_NAME = 'IostatExaWatcher' # module we expect to parse

# for determining max capacity, can only run on the actual host
CELLCLI='cellcli'
COMMAND_CELLCLI="-xml -e list cell attributes maxpdiops,maxpdmbps,maxfdiops,maxfdmbps"

IOSTAT_MSG_01='Unable to retrieve maximum IOPS and MB/s (%s)'
IOSTAT_MSG_02='Not checking for maximum IOPs and MB/s: current host (%s) is processing files extracted from another host (%s)'
#------------------------------------------------------------
def _parse_time_format(line, base_date_str, file_start_time):
  '''
    Parses the sample time in the ExaWatcher IOStat file and returns
    a datetime object constructed from the sample time seen in line

    This understands 4 formats:
      . Time: hh:mi:ss <AM|PM>
      . mm/dd/yyyy hh:mi:ss [AM|PM]
      . mm/dd/yy hh24:mi:ss
      . mm/dd/yyyy hh24:mi:ss

    In the first cases, it converts the AM/PM time to a 24 hour-clock
    then constructs the full timestamp by adding base_date_str to get the
    date component.

    In the other cases, it simply converts the string to a datetime object
    using the proper format mask

    PARAMETERS:
      line: line from ExaWatcher iostatfile
      base_date_str: Starting Time (date component) in ExaWatcher header

    RETURNS:
      datetime object constructed from sample time

  '''
  # FIXME: are there other time formats we need to handle
  # iostat is based on S_TIME_FORMAT if it exists
  # e.g. ISO 8601 - YYYY-MM-DD hh:mm:ss ?
  tokens = line.split()
  if tokens[0] == 'Time:':
    time_str = line.split(None,1)[1]  # format is Time: hh:mi:ss <AM|PM>
    # check if we wrapped around midnight in file based on AM/PM
    time_str_am_pm = time_str.rsplit(None,1)[1]
    start_time_am_pm = file_start_time.strftime('%p')
    if (start_time_am_pm == 'PM' and time_str_am_pm == 'AM'):
      # get a new date by adding 1 day
      base_date = datetime.strptime(base_date_str, '%m/%d/%Y') + timedelta(days=1)
      new_base_date_str = datetime.strftime(base_date,'%m/%d/%Y')
      # add time portion to new date
      sample_time = datetime.strptime(new_base_date_str + ' ' + time_str,
                                      '%m/%d/%Y %I:%M:%S %p')
    else:
      sample_time = datetime.strptime(base_date_str + ' ' + time_str,
                                      '%m/%d/%Y %I:%M:%S %p')
  elif re.match('\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} [AM|PM]',line):
    sample_time = datetime.strptime(line, '%m/%d/%Y %I:%M:%S %p')
  elif re.match('\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}',line):
    sample_time = datetime.strptime(line, '%m/%d/%y %H:%M:%S')
  elif re.match('\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}',line):
    sample_time = datetime.strptime(line, '%m/%d/%Y %H:%M:%S')
    
  return sample_time


#------------------------------------------------------------
def _get_exawatcher_disk_list(line):
  '''
    return the list of flash/hard disk based on the Misc Info line
    in ExaWatcher iostat files
  '''
  line = line.rstrip()  # strip new lines
  line = line.replace('# Misc Info: ','')  # strip the misc info string
  # also remove the /dev/ in all devices
  line = line.replace('/dev/','')

  # get disk and flash
  (disk_str, flash_str) = line.split(';')
  # remove string HardDisk/FlashDisk and split into an array
  disk_list = disk_str.replace('HardDisk: ','').split()
  flash_list = flash_str.replace('FlashDisk: ','').split()

  return (flash_list, disk_list)

#------------------------------------------------------------
def _init_cpustat():
  '''
    initialize object for cpu statistics
  '''
  cpu = { USR: 0, NICE: 0, SYS: 0, WIO: 0, STL: 0, IDL: 0, CNT: 0 }
  return cpu

#------------------------------------------------------------
def _update_cpustat(bucket, usr, nice, sys, wio, steal, idle):
  bucket[USR]  += float(usr)
  bucket[NICE] += float(nice)
  bucket[SYS]  += float(sys)
  bucket[WIO]  += float(wio)
  bucket[STL]  += float(steal)
  bucket[IDL]  += float(idle)
  bucket[CNT]  += 1
  
#------------------------------------------------------------
def _parse_cpu(tokens, bucket_id, hostname, stat_pos, summary):
  '''
    parses the cpu line from iostat and populates the appropriate
    the bucket_id in buckets[hostname]

    PARAMETERS
      tokens   : array created by splitting the line from iostat
      bucket_id: bucket_id where this sample belongs
      hostname : hostname that these stats belong to
      stat_pos : dictionary object indicating position of the stats
      summary  : for calculating average over entire time period
  '''

  if bucket_id not in buckets or hostname not in buckets[bucket_id]:
    msg = 'bucket_id: %d, hostname: %s not initialized' % (bucket_id,hostname)
    _my_report_context.log_msg('error', msg)
    assert False, msg
    
  # (usr, nice, sys, wio, steal, idle) = tokens
  # get stats based on parsed positions
  usr = tokens[stat_pos[USR]]
  nice = tokens[stat_pos[NICE]]
  sys = tokens[stat_pos[SYS]]
  wio = tokens[stat_pos[WIO]]
  steal = tokens[stat_pos[STL]]
  idle = tokens[stat_pos[IDL]]

  bucket = buckets[bucket_id][hostname]

  if CPU not in bucket:
    bucket[CPU] = _init_cpustat()
  if CPU not in summary:
    summary[CPU] = _init_cpustat()

  # now update stats; we keep incrementing and will get average
  # at the end
  _update_cpustat(bucket[CPU], usr, nice, sys, wio, steal, idle)
  _update_cpustat(summary[CPU], usr, nice, sys, wio, steal, idle)

#------------------------------------------------------------
def _init_diskstat():
  '''
    initializes the dictionary object for a disk
  '''
  device =  { RPS: 0, WPS: 0,
              RMBPS: 0, WMBPS: 0,
              AVGRQSZ: 0, AVGQUSZ: 0,
              AWAIT: 0, SVCTM: 0,
              UTIL: 0, CNT: 0 }
  return device

#---------------------------------------------------------------------
def _update_diskstat(disk, rps, wps, rsecps, wsecps,
                    avgrqsz, avgqusz, await, svctm, util):
  '''
    updates the stats for the given disk
  '''
  disk[RPS]     += float(rps)
  disk[WPS]     += float(wps)
  disk[RMBPS]   += float(rsecps)*512/1048576  # convert to MBPS
  disk[WMBPS]   += float(wsecps)*512/1048576 # convert to MBPS
  disk[AVGRQSZ] += float(avgrqsz)
  disk[AVGQUSZ] += float(avgqusz)
  disk[AWAIT]   += float(await)
  disk[SVCTM]   += float(svctm)
  disk[UTIL]    += float(util)
  disk[CNT]     += 1

#------------------------------------------------------------
def _parse_disk(tokens, bucket_id, is_flash, is_disk, hostname, stat_pos,
                summary):
  '''
    parses the line from iostat that has the device statistics
    and updates the buckets[bucket_id] for the device
    PARAMETERS:
      tokens   : array created by splitting the line from iostat
      bucket_id: bucket_id where this sample belongs
      is_flash : boolean - True if this device is a flash disk
      is_disk  : boolean - True if this device is a hard disk
      hostname : hostname that stats belong to
      stat_pos : dictionary object indicating position of the stats
                 we need this since we can sometimes have a different
                 set of stats based on iostat command
      summary  : for calculating overall average for the entire period
  '''

  if bucket_id not in buckets or hostname not in buckets[bucket_id]:
    msg = 'bucket_id: %d, hostname: %s not initialized' % (bucket_id,hostname)
    _my_report_context.log_msg('error', msg)
    assert False, msg

  # split line into its component stats
  # default
  #  (device, rrqmps, wrqmps, rps, wps, rsecps, wsecps, avgrqsz, avgqusz,
  #     await, svctm, util) = tokens;
  # may have different formats, so we figure out position based on what
  # we parsed in 'Device:' line, assume device is always first position though
  device = tokens[0]
  rps = tokens[stat_pos[RPS]]
  wps = tokens[stat_pos[WPS]]
  rsecps = tokens[stat_pos[RSECPS]]
  wsecps = tokens[stat_pos[WSECPS]]
  avgrqsz = tokens[stat_pos[AVGRQSZ]]
  avgqusz = tokens[stat_pos[AVGQUSZ]]
  await = tokens[stat_pos[AWAIT]]
  svctm = tokens[stat_pos[SVCTM]]
  util = tokens[stat_pos[UTIL]]
  

  bucket = buckets[bucket_id][hostname]

  # determine if device should be in FLASH or DISK
  if is_flash:
    if FLASH not in bucket:
      bucket[FLASH] = {}
    if FLASH not in summary:      
      summary[FLASH] = {}
    disk_type = FLASH  
    bucket_diskgroup = bucket[FLASH]
  else:
    if DISK not in bucket:
      bucket[DISK] = {}
    if DISK not in summary:
      summary[DISK] = {}
    disk_type = DISK
    bucket_diskgroup = bucket[DISK]

  if device not in bucket_diskgroup:
    bucket_diskgroup[device] = _init_diskstat()

  if device not in summary[disk_type]:
    summary[disk_type][device] = _init_diskstat()
    
  _update_diskstat(bucket_diskgroup[device],
                   rps, wps, rsecps, wsecps,
                   avgrqsz, avgqusz,
                   await, svctm, util)

  # also update summary bucket with running total
  _update_diskstat(summary[disk_type][device],
                   rps, wps, rsecps, wsecps,
                   avgrqsz, avgqusz,
                   await, svctm, util)

#------------------------------------------------------------
def _compute_cpu_bucket(cpu):
  '''
    computes average cpu usage in the bucket
    PARAMETERS:
      cpu - bucket[host][CPU] object from buckets
  '''

  if cpu[CNT] > 1:
    cpu[USR] = cpu[USR]/cpu[CNT]
    cpu[SYS] = cpu[SYS]/cpu[CNT]
    cpu[WIO] = cpu[WIO]/cpu[CNT]
    cpu[IDL] = cpu[IDL]/cpu[CNT]
  # always compute cpu busy, regardless of number of entries in bucket  
  cpu[BUSY] = 100 - cpu[IDL]

#------------------------------------------------------------
def _compute_disk_bucket(disklist):
  '''
     computes average disk stats for each device per bucket
     and also computes the SUMMARY object for FLASH/DISK
     Note: for IOPS, MBPS, we compute the aggregate across all FLASH/DISK
           for others, we get the average
     PARAMETERS
       disklist - bucket[host][FLASH/DISK] dictionary object from buckets
  '''
  total = _init_diskstat()
  # initialize derived stats
  total[IOPS] = 0
  total[MBPS] = 0

  # set up summary
  for disk in disklist:
    # TODO: do we need to check for divide-by-zero? shouldn't really happen
    cnt = disklist[disk][CNT]
    total[CNT] += cnt
    # per disk stats
    rrq_ps = disklist[disk][RPS]/cnt
    wrq_ps = disklist[disk][WPS]/cnt
    iorq_ps = rrq_ps + wrq_ps
    rmb_ps = disklist[disk][RMBPS]/cnt
    wmb_ps = disklist[disk][WMBPS]/cnt
    iomb_ps = rmb_ps + wmb_ps

    # compute aggregate for total bucket
    total[RPS] += rrq_ps
    total[WPS] += wrq_ps
    total[IOPS] += iorq_ps
    total[RMBPS] += rmb_ps
    total[WMBPS] += wmb_ps
    total[MBPS] += iomb_ps

    # and keep running total for later computation of average
    total[AWAIT] += disklist[disk][AWAIT]
    total[SVCTM] += disklist[disk][SVCTM]
    total[UTIL] += disklist[disk][UTIL]

    # overwrite the information for the disk
    # with the average rates for the bucket
    disklist[disk][RPS] = rrq_ps
    disklist[disk][WPS] = wrq_ps
    disklist[disk][IOPS] = iorq_ps
    disklist[disk][RMBPS] = rmb_ps
    disklist[disk][WMBPS] = wmb_ps
    disklist[disk][MBPS] = iomb_ps
    disklist[disk][AWAIT] = disklist[disk][AWAIT]/cnt
    disklist[disk][SVCTM] = disklist[disk][SVCTM]/cnt
    disklist[disk][UTIL] = disklist[disk][UTIL]/cnt

  # now finalize the total/summary bucket - we want averages for
  # the following 3 stats, but keep aggregates for IOPS and MBPS
  total[AWAIT] = total[AWAIT]/total[CNT]
  total[SVCTM] = total[SVCTM]/total[CNT]
  total[UTIL]  = total[UTIL]/total[CNT]

  disklist[SUMMARY] = total

#------------------------------------------------------------
def _get_max_capacity(report_context,hostname):
  # get max capacity using cellcli
  # will set HostMetadataIostat object, so we can eventually use
  # the information as part of reference lines
  # call cellcli
  # and then populate HostMetadataIostat.capacity
  # caveat, if this is EF, we may not get the maxPDIOPS or maxPDMBPS
  # in the xml
  # call cellcli
  if not distutils.spawn.find_executable(CELLCLI):
    report_context.hostnames[hostname].iostat.add_finding(IOSTAT_MSG_01 % ('No cli command'), FINDING_TYPE_INFO)
    return

  # otherwise, we continue on
  root_xml = None
  try:
    p = Popen([ CELLCLI, COMMAND_CELLCLI], stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=False)
    output, err = p.communicate()
    rc = p.returncode

    if rc != 0:
      # report unable to get max capacity and its reason
      report_context.hostnames[hostname].iostat.add_finding(IOSTAT_MSG_01 % (output + err), FINDING_TYPE_INFO)
      return

    output = output.lstrip()
    if len(output.strip()) == 0:
      report_context.hostnames[hostname].iostat.add_finding(IOSTAT_MSG_01 % ('could not determine capacity'), FINDING_TYPE_INFO)
      return

    # parse the XML
    cell_xml = etree.fromstring(output).find('cell')
    
    # note: we assume at this point, hostnames has already been populated
    # extract attributes
    if cell_xml.find('maxFDMBPS') is not None or cell_xml.find('maxFDIOPS') is not None:
      if hostnames[hostname].capacity == None:
        hostnames[hostname].capacity = {}
      hostnames[hostname].capacity[FLASH] = { IOPS: float(cell_xml.find('maxFDIOPS').text),
                                              MBPS: float(cell_xml.find('maxFDMBPS').text) }
      
    if cell_xml.find('maxPDMBPS') is not None or cell_xml.find('maxPDIOPS') is not None:
      if hostnames[hostname].capacity == None:
        hostnames[hostname].capacity = {}
      hostnames[hostname].capacity[DISK] = { IOPS: float(cell_xml.find('maxPDIOPS').text),
                                              MBPS: float(cell_xml.find('maxPDMBPS').text) }

  except OSError as e:
    report_context.hostnames[hostname].iostat.add_finding(IOSTAT_MSG_01 % (str(e)), FINDING_TYPE_INFO)
    report_context.log_msg('warning', 'Unable to determine max capacity (%s)' % str(e))
  except Exception as e:
    report_context.hostnames[hostname].iostat.add_finding(IOSTAT_MSG_01 % (str(e)), FINDING_TYPE_INFO)
    report_context.log_msg('warning', 'Unable to determine max capacity (%s: %s)' % (e.__class__.__name__ , str(e)))


#------------------------------------------------------------
def _process_rules(report_context):

  # list of callbacks
  RULES_IOSTAT = [ exawrules.rule_iostat_01_high_await ,
                   exawrules.rule_iostat_02_high_util ,
                   exawrules.rule_iostat_03_max_iops ,
                   exawrules.rule_iostat_04_max_mbps ,
                   exawrules.rule_iostat_05_individual_disks ]
                     
  current_hostname = get_hostname()

  for host in report_context.hostnames:
    # ignore multi-cell information
    if host == '':
      continue

    # check if current host, and if so, get max capacity info
    if host == current_hostname:
      _get_max_capacity(report_context, host)
    else:
      # add information (not actual finding)
      report_context.hostnames[host].iostat.add_finding(IOSTAT_MSG_02 % (current_hostname, host), FINDING_TYPE_INFO)

    for rule in RULES_IOSTAT:
      # build a tuple with other relevant information
      info = ( hostnames[host] , )
      rule(report_context.hostnames[host].iostat, info)

    report_context.log_msg('debug','%s iostat findings: %s' % (host,
                                                               report_context.hostnames[host].iostat.findings))
    
#------------------------------------------------------------
def parse_input_files(filelist,
                      report_context,
                      flash_disks_user = DEFAULT_FLASH_DISKS,
                      hard_disks_user = DEFAULT_HARD_DISKS):


  '''
    This is the main routine in this module, which parses the
    files and populates the buckets

    PARAMETERS:
      filelist  : list of files to process, can be bz2, gz or text
      report_context: report context with start/end times and bucket
                  information
      flash_disks_user: list of flash disks (optional); only used
                  if list is not in the header file of exawatcher iostat
      hard_disks_user: list of hard disks (optional); only used
                  if list is not in the header file of exawatcher stats

    NOTES:
      flash_disks_user, hard_disks_user - uses DEFAULT if not specified

    DESCRIPTION:
      This will set the following global variables
        buckets - dictionary object keyed by bucket_id with datapoints
        hostnames: where each metadata object includes
          hostname
          processed_files
          flash_disks - list of all flash devices for host
          hard_disks - list of all hard disk devices per host
        Note, that the devices can be different per file, so we need to
        maintain a full list to make sure we show all relevant devices
        in the chart
        The individual bucket will have a list of FLASH/DISK for that
        bucket.

    As we parse the file, the datapoints are accumulated in each bucket.
    After parsing, we go through a second pass to compute the average
    within each bucket.  (Note: we do this so that after parsing,
    any module - i.e. using gnuplot or google charts, can simply
    plot the data without having to calculate averages)
    Note: if a bucket contains data from two files, and if the files
    had different devices in them, make sure we still calculate
    this correctly by maintaining the COUNT within the device

    We also maintain a list of processed_start_times - this is based on the
    hostname and 'Starting Time' string at the start of the exawatcher
    iostat file.
    If we see the same 'Starting Time' (for the same host) we skip the
    file and move onto the next file

    We also check if file has data for the timeframe of interest, if not
    we skip the file

  '''
  # global variables that will be set
  global buckets
  global hostnames
  global _my_report_context

  _my_report_context = report_context

  # list of file start_times we have processed - based on header in file
  processed_start_times = []
  
  # now go through the list of files
  for fname in (filelist):

    try:
      # determine type of file, only process if we recognize the filetype
      ftype = file_type(fname,_my_report_context)
      input_file = open_file(fname, ftype)
      if ftype == FILE_UNKNOWN or input_file == None:
        raise UnrecognizedFile(fname + '(' + ftype + ')')

      # get hostname
      hostname = get_hostname_from_filename(fname)

      # first check file header to ensure this is an ExaWatcher iostat file
      header = [next(input_file) for x in xrange(EXAWATCHER_HEADER_LINES)]

      # we expect the module to be the 4th line and we will look for it there
      if EXAWATCHER_IOSTAT_MODULE_NAME not in header[EXAWATCHER_MODULE_POSITION]:
        # skip this file
        raise UnrecognizedFile(fname)

      # check if we have processed this file based on start time
      if (hostname,header[EXAWATCHER_STARTING_TIME_POSITION]) in processed_start_times:
        raise DuplicateFile(fname)

      # extract Starting Time from ExaWatcher header, and get last two
      # strings after split()
      # we need to get the date in case the time format only has hh:mi:ss
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
      _my_report_context.log_msg('warning', 'Ignoring duplicate file: %s' %(e.value))
    except NoDataInFile as e:
      _my_report_context.log_msg('warning', 'No data within report interval in file: %s' % (e.value))
    except IOError as e:
      if e.errno == errno.EACCES:
        _my_report_context.log_msg('error', 'No permissions to read file: %s (%s)' % (fname, str(e)))
      else:
        _my_report_context.log_msg('error', 'Unable to process file: %s: %s' % (fname, str(e)))
    except Exception as e:
      _my_report_context.log_msg('error', 'Unable to process file: %s: %s' % (fname,
                                                              str(e)))
    else:
      # only append if we will be processing the file
      if hostname not in hostnames:
        hostnames[hostname] = HostMetadataIostat(hostname)

      # also make sure we have this in our report context
      if hostname not in _my_report_context.hostnames:
        _my_report_context.add_hostinfo(hostname)
        
      # include in list to keep track of files processed
      processed_start_times.append( (hostname,header[EXAWATCHER_STARTING_TIME_POSITION]) )
      hostnames[hostname].processed_files.append(fname)

      # get the disk list from exawatcher if available
      if 'Misc Info' in header[EXAWATCHER_MISC_INFO_POSITION]:
        (file_flash_disks, file_hard_disks) = _get_exawatcher_disk_list(header[EXAWATCHER_MISC_INFO_POSITION])
      # otherwise use default
      else:
        file_flash_disks = flash_disks_user
        file_hard_disks  = hard_disks_user
        # and print out warning that we are using the default list
        # although if a user specifies it and it is identical then it will
        # still print out this message ...
        if file_flash_disks == DEFAULT_FLASH_DISKS:
            _my_report_context.log_msg('info', 'Using defaults for flash disks (or specified list is same as default')
        if file_hard_disks == DEFAULT_HARD_DISKS:
            _my_report_context.log_msg('info', 'Using defaults for hard disks (or specified list is same as default)')
      # for any that aren't yet in our list, add them
      # fortify: revalidate the diskname
      for fdisk in sorted(file_flash_disks):
        if fdisk not in hostnames[hostname].flash_disks:
          diskname = validate_disk(fdisk)
          if diskname != None:
            hostnames[hostname].flash_disks.append(diskname)
      for hdisk in sorted(file_hard_disks):
        if hdisk not in hostnames[hostname].hard_disks:
          diskname = validate_disk(hdisk)
          if diskname != None:
            hostnames[hostname].hard_disks.append(diskname)

      # new file, reset position of stats
      get_disk_stat_pos = True
      get_cpu_stat_pos = True
      # default positions in line
      disk_stat_pos = { RPS: None , WPS: None , RSECPS: None , WSECPS: None ,
                        AVGRQSZ: None , AVGQUSZ: None ,
                        AWAIT: None , SVCTM: None ,
                        UTIL: None  }
      cpu_stat_pos = { USR: None , NICE: None ,
                       SYS: None , WIO: None ,
                       STL: None , IDL: None }
      
      # initialize bucket_id
      bucket_id = -1

      # now process the rest of the file
      for line in input_file:
        line = line.rstrip()  # remove newline
        tokens = line.split() # split into tokens

        # skip blank lines
        if len(tokens) == 0:
          continue

        # older version has Time in each line
        # newer version has mm/dd/yy hh24:mi:ss
        # or                mm/dd/yyyy hh:mi:ss AM|PM
        if tokens[0] == 'Time:' or re.match('\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}',line) or re.match('\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}',line):
          sample_time = _parse_time_format(line,file_start_date_str, file_start_time)

          # for samples in our desired range, get the bucket_id
          if sample_time >= report_context.report_start_time and sample_time <= report_context.report_end_time:
            bucket_id = report_context.get_bucket_id(sample_time)
            # create bucket with hostname
            if bucket_id not in buckets:
              buckets[bucket_id] = { }
            if hostname not in buckets[bucket_id]:
              buckets[bucket_id][hostname] = {}
          else:
            bucket_id = -1

        elif tokens[0] == 'avg-cpu:':
          # get position of stats for this file,
          # subtract 1 since we dont' have the avg-cpu line in the actual stats
          if get_cpu_stat_pos:
            try:
              cpu_stat_pos[USR] = tokens.index('%user') - 1
              cpu_stat_pos[NICE] = tokens.index('%nice') - 1
              cpu_stat_pos[SYS] = tokens.index('%system') - 1
              cpu_stat_pos[WIO] = tokens.index('%iowait') - 1
              cpu_stat_pos[STL] = tokens.index('%steal') - 1
              cpu_stat_pos[IDL] = tokens.index('%idle') - 1
              get_cpu_stat_pos = False
            except ValueError as e:
              _my_report_context.log_msg('error','Unable to parse cpu statistics for file: %s (%s)' % (fname, str(e)))
              raise
            
          # we know cpu is coming
          state_cpu = True

        # this is the CPU line if it has 6 tokens ...
        elif len(tokens) == 6 and state_cpu:
          if bucket_id != -1:
            _parse_cpu(tokens,bucket_id,hostname, cpu_stat_pos,
                       report_context.hostnames[hostname].iostat.summary_stats)
          state_cpu = False

        # get stat positions for disk
        elif tokens[0] == 'Device:' and get_disk_stat_pos:
          try:
            disk_stat_pos[RPS] = tokens.index('r/s')
            disk_stat_pos[WPS] = tokens.index('w/s')
            disk_stat_pos[RSECPS] = tokens.index('rsec/s')
            disk_stat_pos[WSECPS] = tokens.index('wsec/s')
            disk_stat_pos[AVGRQSZ] = tokens.index('avgrq-sz')
            disk_stat_pos[AVGQUSZ] = tokens.index('avgqu-sz')
            disk_stat_pos[AWAIT] = tokens.index('await')
            disk_stat_pos[SVCTM] = tokens.index('svctm')
            disk_stat_pos[UTIL] = tokens.index('%util')
            get_disk_stat_pos = False
          except:
            _my_report_context.log_msg('error','Unable to parse disk statistics for file: %s (%s)' % (fname, str(e)))
            raise
          
        # we only consider disks that are specified as flash/hard disks
        # for this one file
        elif (tokens[0] in file_flash_disks or tokens[0] in file_hard_disks) and bucket_id != -1:
          _parse_disk(tokens, bucket_id,
                      (tokens[0] in file_flash_disks),
                      (tokens[0] in file_hard_disks),
                      hostname,
                      disk_stat_pos,
                      report_context.hostnames[hostname].iostat.summary_stats)
    finally:
      # close the file
      if input_file != None:
        input_file.close()

  # once we have buckets, make a second pass to compute data
  # so consumers can use buckets as-is and print it out as
  # necessary
  # FUTURE OPTIMIZATION: ?
  # if we sort the input (or guarantee sorted input), we
  # could potentially avoid this second pass and calculate
  # the averages whenever we move buckets

  # determine if multiple hosts
  if len(hostnames) > 1:
    _my_report_context.set_multihost(True)

  for (i,bucket) in buckets.iteritems():
    for host in bucket:

      if CPU in bucket[host]:
        _compute_cpu_bucket(bucket[host][CPU])

      for disktype in [ FLASH, DISK ]:
        if disktype in bucket[host]:
          _compute_disk_bucket(bucket[host][disktype])

  # now calculate averages for the summary bucket
  for host in report_context.hostnames:
    summary_stats = report_context.hostnames[host].iostat.summary_stats
    if CPU in summary_stats:
      _compute_cpu_bucket(summary_stats[CPU])
    for disktype in [ FLASH, DISK ]:
      if disktype in summary_stats:
        _compute_disk_bucket(summary_stats[disktype])

  _process_rules(report_context)

#------------------------------------------------------------
def main():
  global _my_report_context
  _my_report_context = ReportContext()
  # not expected to be called on its own ...except for potential unit
  # tests
  _my_report_context.log_msg('error', 'exaparse main noop')

#
#------------------------------------------------------------
# standard template
#------------------------------------------------------------
if __name__ == '__main__':
  main()
