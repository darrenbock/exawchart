#!/usr/bin/python
#
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
#
#     NAME
#       exawrules.py
#
#     DESCRIPTION
#       Rules used when analyzing exawatcher data
#       Each module should declare the list of rules (functions) that it
#       should process and should pass the appropriate arguments to each of
#       the rules
#       
#       Each rule should have all the data it needs to process the rule
#
#
#     MODIFIED   (MM/DD/YY)
#     cgervasi    12/01/16 - typo in MPSTAT
#     cgervasi    10/03/16 - Creation
#
#     NOTES:
#       Rules are centralized in this file
#       . Each rule needs to be passed the StatFileSummary object for the
#         filetype (i.e report_context.hostnames[host].<filetype>
#       . An (optional) second parameter with additional information can
#         be passed in as well.  This second parameter is a tuple,
#         and the rule (and its callers) have to know the list of data
#         in this tuple.  Note, for simplicity, even singleton parameters
#         should be passed as a tuple
#
#       Eventually, this may be extended to process multiple hosts
#
#       Should the rules be encapsulated in each module, or should we just
#       have a generic location with all the rules?
#
#       Generic location seems to be easier, so we can review all rules at once
#       However, the structure of each StatFileSummary.summary_stats is
#       dependent on the module
#
#       Eventually, we could define classes for each one, however its hashkey
#       will always be dependent on the processed information (ie. devicename,
#       cpu id, cellsrvstat metric key ...)

# import some constants from exawutil
from exawutil import USR, SYS, WIO, IDL, IOPS, MBPS, AWAIT, UTIL, FLASH, DISK, SUMMARY, VALUE, FINDING_TYPE_INFO, FINDING_TYPE_SUMMARY, FINDING_TYPE_DETAIL

# These are findings we can process
# NOTE: no globalization; any globalization if required should be done in the UI
FINDING_MPSTAT_MSG_01='High CPU Usage: %.2f'
FINDING_MPSTAT_MSG_02='High CPU Usage on %d (of %d) CPUs'
FINDING_MPSTAT_MSG_03='High CPU Usage with low %%usr on %d (of %d) CPUs'

FINDING_ALERT_MSG_01='No alerts'
FINDING_ALERT_MSG_02='Alerts found during report interval: critical: %d | warning: %d | info:%d'

# findings for iostat
FINDING_IOSTAT_MSG_01='%s: High average wait times: %.2fms'
FINDING_IOSTAT_MSG_02='%s: High utilization %.2f%%'
FINDING_IOSTAT_MSG_03='%s: %.2f IOPs exceeds maximum IOPs capacity of %d'
FINDING_IOSTAT_MSG_04='%s: %.2f MB/s exceeds maximum MB/s capacity of %d'
FINDING_IOSTAT_MSG_05='%s: %d devices have average wait times exceeding %.2fms'
FINDING_IOSTAT_MSG_06='%s: %d devices have average utilization exceeding %.2f%%'
FINDING_IOSTAT_MSG_07='%s: %d devices exceeds maximum IOPs capacity of %d'
FINDING_IOSTAT_MSG_08='%s: %d devices exceeds maximum MB/s capacity of %d'

FINDING_CELLSRVSTAT_MSG_01='%d memory allocation failures'
FINDING_CELLSRVSTAT_MSG_02='%.2f MB of Smart IO passthru (%.2f eligible MB)'
FINDING_CELLSRVSTAT_MSG_03='%.2f OLTP Hit ratio on flash cache'

# hard-coded threshold for average wait times and utilization
RULE_IOSTAT_AWAIT_THRESHOLD={ FLASH: 10, DISK: 20 }
RULE_IOSTAT_UTIL_THRESHOLD={ FLASH: 80, DISK: 80 }

RULE_CELLSRVSTAT_FC_HIT_RATIO=80

#------------------------------------------------------------
def rule_alert_01_count(summary_item, info = None):
  alert_summary = summary_item.summary_stats
  if alert_summary['critical'] + alert_summary['warning'] + alert_summary['info'] > 0:
    msg = FINDING_ALERT_MSG_02 % (alert_summary['critical'],
                                  alert_summary['warning'],
                                  alert_summary['info'])
    summary_item.add_finding(msg)
  else:
    summary_item.add_finding(FINDING_ALERT_MSG_01, FINDING_TYPE_INFO)

#------------------------------------------------------------
def rule_mpstat_01_high_cpu(summary_item, info):
  mpstat_summary = summary_item.summary_stats
  (idle_threshold,) = info
  if 'all' in mpstat_summary and mpstat_summary['all'][IDL] < idle_threshold:
    msg = FINDING_MPSTAT_MSG_01 % float(100-mpstat_summary['all'][IDLE])
    summary_item.add_finding(host, 'mpstat', msg)

#------------------------------------------------------------
def rule_mpstat_02_high_cpu_subset_alert(summary_item,info):
  mpstat_summary = summary_item.summary_stats
  # extract summary information
  (host_metadata, num_cpus) = info
  if len(host_metadata.flag_alert) > 0:
    summary_item.add_finding(FINDING_MPSTAT_MSG_03 % (len(host_metadata.flag_alert),
                                               num_cpus))
#------------------------------------------------------------
def rule_mpstat_03_high_cpu_subset_warning(summary_item, info):
  mpstat_summary = summary_item.summary_stats
  # extract summary information
  (host_metadata, num_cpus) = info
  if len(host_metadata.flag_warning) > 0:
    summary_item.add_finding(FINDING_MPSTAT_MSG_02 % (len(host_metadata.flag_warning),
                                               num_cpus))


#----------------------------------------------------------------------
def rule_iostat_01_high_await(summary_item, info = None):
  iostat_summary = summary_item.summary_stats
  for disktype in [ FLASH, DISK ]:
    if disktype in iostat_summary and SUMMARY in iostat_summary[disktype]:
      if iostat_summary[disktype][SUMMARY][AWAIT] > RULE_IOSTAT_AWAIT_THRESHOLD[disktype]:
          summary_item.add_finding(FINDING_IOSTAT_MSG_01 % (disktype,
                                                            iostat_summary[disktype][SUMMARY][AWAIT]))

#----------------------------------------------------------------------
def rule_iostat_02_high_util(summary_item, info = None):
  iostat_summary = summary_item.summary_stats
  for disktype in [ FLASH, DISK ]:
    if disktype in iostat_summary and SUMMARY in iostat_summary[disktype]:
      if iostat_summary[disktype][SUMMARY][UTIL] > RULE_IOSTAT_UTIL_THRESHOLD[disktype]:
          summary_item.add_finding(FINDING_IOSTAT_MSG_02 % (disktype,
                                                            iostat_summary[disktype][SUMMARY][UTIL]))

#----------------------------------------------------------------------
def rule_iostat_03_max_iops(summary_item, info):
  iostat_summary = summary_item.summary_stats
  (host_metadata,) = info
  for disktype in [ FLASH, DISK ]:
    if host_metadata.capacity != None and disktype in host_metadata.capacity and disktype in iostat_summary and SUMMARY in iostat_summary[disktype] and iostat_summary[disktype][SUMMARY][IOPS] > host_metadata.get_cell_capacity(disktype, IOPS):
      summary_item.add_finding(FINDING_IOSTAT_MSG_03 % (disktype,
                                                        iostat_summary[disktype][SUMMARY][IOPS],
                                                        host_metadata.get_cell_capacity(disktype, IOPS)))

#----------------------------------------------------------------------
def rule_iostat_04_max_mbps(summary_item, info):
  iostat_summary = summary_item.summary_stats
  (host_metadata,) = info
  for disktype in [ FLASH, DISK ]:
    if host_metadata.capacity != None and disktype in host_metadata.capacity and disktype in iostat_summary and SUMMARY in iostat_summary[disktype] and iostat_summary[disktype][SUMMARY][MBPS] > host_metadata.get_cell_capacity(disktype, MBPS):
      summary_item.add_finding(FINDING_IOSTAT_MSG_04 % (disktype,
                                                        iostat_summary[disktype][SUMMARY][MBPS],
                                                        host_metadata.get_cell_capacity(disktype, MBPS)))


#----------------------------------------------------------------------
def rule_iostat_05_individual_disks(summary_item, info):
  # this rule will count the number of disks that exceed the threshold
  # we go through the data once, so we do all counts here
  iostat_summary = summary_item.summary_stats
  (host_metadata,) = info
  capacity = host_metadata.capacity
  for disktype in [ FLASH, DISK ]:
    disk_count = { IOPS: 0, MBPS: 0, AWAIT: 0, UTIL: 0 }
    if disktype in iostat_summary:
      for disk in sorted(iostat_summary[disktype]):
        # skip summary bucket, this is overall information for the cell  
        if disk == SUMMARY:
          continue
        # check against maximum capacity if available
        if capacity != None and disktype in capacity:
          if iostat_summary[disktype][disk][IOPS] > capacity[disktype][IOPS]:
            disk_count[IOPS] += 1
          if iostat_summary[disktype][disk][MBPS] > capacity[disktype][MBPS]:
            disk_count[MBPS] += 1
        if iostat_summary[disktype][disk][AWAIT] > RULE_IOSTAT_AWAIT_THRESHOLD[disktype]:
          disk_count[AWAIT] += 1
        if iostat_summary[disktype][disk][UTIL] > RULE_IOSTAT_UTIL_THRESHOLD[disktype]:
          disk_count[UTIL] += 1

      # once we  have counted all disks for this disk type, add msgs     
      if disk_count[IOPS] > 0:
        summary_item.add_finding(FINDING_IOSTAT_MSG_07 % (disktype,
                                                          disk_count[IOPS],
                                                          capacity[disktype][IOPS]),
                                 FINDING_TYPE_DETAIL)
      if disk_count[MBPS] > 0:
        summary_item.add_finding(FINDING_IOSTAT_MSG_08 % (disktype,
                                                          disk_count[MBPS],
                                                          capacity[disktype][MBPS]),
                                 FINDING_TYPE_DETAIL)
      if disk_count[AWAIT] > 0:
        summary_item.add_finding(FINDING_IOSTAT_MSG_05 % (disktype,
                                                          disk_count[AWAIT],
                                                          RULE_IOSTAT_AWAIT_THRESHOLD[disktype]),
                                 FINDING_TYPE_DETAIL)
      if disk_count[UTIL] > 0:
        summary_item.add_finding(FINDING_IOSTAT_MSG_06 % (disktype,
                                                          disk_count[UTIL],
                                                          RULE_IOSTAT_UTIL_THRESHOLD[disktype]),
                                 FINDING_TYPE_DETAIL)        

#------------------------------------------------------------
def rule_cellsrvstat_01_mem_failures(summary_item, info):
  cs_summary = summary_item.summary_stats
  #info has list of keys to process
  mem_failures = 0
  for key in info:
    mem_failures += cs_summary[key][VALUE]
  if mem_failures > 0:
    summary_item.add_finding(FINDING_CELLSRVSTAT_MSG_01 % mem_failures)

#------------------------------------------------------------
def rule_cellsrvstat_02_sio_pt(summary_item, info):
  cs_summary = summary_item.summary_stats
  (elig_key, cpu_key, pt_key) = info
  # make sure there's at least some activity
  if cs_summary[cpu_key][VALUE] + cs_summary[pt_key][VALUE] > 0.01 and cs_summary[elig_key] > 0.01:
    summary_item.add_finding(FINDING_CELLSRVSTAT_MSG_02 % (
        (cs_summary[cpu_key][VALUE] + cs_summary[pt_key][VALUE]),
        cs_summary[elig_key][VALUE]))

#------------------------------------------------------------
def rule_cellsrvstat_03_fc_oltp_hit(summary_item, info):
    cs_summary = summary_item.summary_stats
    (rhit_key,rmiss_key) = info
    rhit = cs_summary[rhit_key][VALUE]
    rmiss = cs_summary[rmiss_key][VALUE]
    if (rhit + rmiss > 0):
      hit_ratio = 100*(rhit/(rhit + rmiss))
      if hit_ratio < RULE_CELLSRVSTAT_FC_HIT_RATIO:
        summary_item.add_finding(FINDING_CELLSRVSTAT_MSG_03 % hit_ratio)
