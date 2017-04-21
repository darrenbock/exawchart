#!/usr/bin/python
#
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
#
#     NAME
#       exawchart_mp.py
#
#     DESCRIPTION
#       Creates chart for mpstat data per cpu
#
#     MODIFIED   (MM/DD/YY)
#     cgervasi    09/28/16 - add summary page
#     cgervasi    09/14/16 - Creation
#

import getopt
import sys
import re
import os
import exawparse_mp
import json

from datetime import datetime, timedelta
from glob import glob

from exawutil import USR, NICE, SYS, WIO, STL, IDL, BUSY, DATE_FMT_INPUT, JSON_DATE_FMT, DEFAULT_MAX_BUCKETS, add_empty_point, add_start_end_times, ReportContext, HostMetadata

from exawparse_mp import IRQ, SOFT, GUEST

# change json to only dump 6 decimal points for float
json.encoder.FLOAT_REPR = lambda o: format(o, '.6f')

#------------------------------------------------------------    
def _get_stat_label(stat):
  label = stat
  if stat == USR:
    label = '%usr'
  elif stat == NICE:
    label = '%nice'
  elif stat == SYS:
    label = '%sys'
  elif stat == WIO:
    label = '%wio'
  elif stat == STL:
    label = '%steal'
  elif stat == IRQ:
    label = '%irq'
  elif stat == SOFT:
    label = '%soft'
  elif stat == GUEST:
    label = '%guest'
  elif stat == IDL:
    label = '%idle'
  elif stat == BUSY:
    label = '%busy'
  return label

#------------------------------------------------------------
def _get_stat_color(stat):
  if stat == USR:
    return '#00CC00'
  elif stat == SYS:
    return '#D2691E'
  elif stat == WIO:
    return '#0094E7'
  else:
    return None
    
#------------------------------------------------------------
def _print_cpu_id_chart(report_context,
                        summary,
                        host_metadata):
  '''
    calculates the cpu utilization for each CPU based on the summary
    bucket
    PARAMETERS:
      report_context: ReportContext to process, includes time range,
                      bucket interval, num_buckets
      summary: summary bucket which has information for each cpu id
      host_metadata: HostMetadata object from parsing mpstat
                      
  '''
  hostname = host_metadata.name

  #Required data structure
  # xAxis - list of cpu ids
  # data is keyed by stat name, array contains value for the corresponding cpu
  # data = { USR: [ <cpu0 %usr>, <cpu1 %usr>, <cpu2 %usr> .... ],
  #          NICE: [ <cpu0 %nice>, <cpu1 %nice>, <cpu2 %nice> .... ],
  #        ... }               

  # stats to process
  stats = [ USR, NICE, SYS, WIO, IRQ, SOFT, STL, GUEST, IDL ]

  xAxis = []  # xAxis will be cpu_ids
  data = {}   # keyed by stat so we can bind it to series later on

  # all ids should be ints except for 'all' which is a string
  # we expect 'all' should get sorted as the last element
  for cpu_id in sorted(summary):

    # append cpu id to our xAxis  
    xAxis.append(cpu_id)

    # now get the individual stats
    for stat in stats:
      if stat not in data:
        data[stat] = []
      if stat in summary[cpu_id] and summary[cpu_id][stat] != None:
        # divide by 100, since charting utility multiples by 100 for %
        data[stat].append( summary[cpu_id][stat]/100  )
      else:
        data[stat].append( None )

  # create the array that will be bound to the series in the UI
  seriesData = []
  for stat in stats:
    if stat in data:
      seriesItem =  { 'id': stat,
                      'items': data[stat],
                      'name': _get_stat_label(stat) }
      if stat == USR or stat == SYS or stat == WIO:
        seriesItem['color'] = _get_stat_color(stat)
      elif stat == IDL:
        seriesItem['visibility'] = 'hidden'
      seriesData.append(seriesItem)

  # return it to caller, caller will write out html file
  return (xAxis, seriesData);

#------------------------------------------------------------
def _print_all_chart(report_context,
                     buckets,
                     host_metadata):
  '''
    gets data to be able to display a timeline of cpu usage
    this only displays 'all' cpu, along with outlier cpus
    (those with warning or alerts)
    PARAMETERS:
      report_context : ReportContext to process, includes time range,
                       bucket interval, num_buckets
      buckets        : parsed result of mpstat data, in buckets
      host_metadata: HostMetadata object from parsing mpstat
        
  '''
  # get hostname
  hostname = host_metadata.name
  
  xAxis = [] # list of xAxis times
  data = {}  # keyed by stat, will be items property when we build series
  # structure
  # data: { cpu_id: { USR: [ <values corresponding to timeline > ],
  #                  NICE: [ <values corresponding to timeline> ] , ... }
  #
  stats = [ USR, NICE, SYS, WIO, IRQ, SOFT, STL, GUEST, IDL ]

  # first get list of cpus
  cpu_ids = [ 'all' ] + sorted(host_metadata.flag_alert + host_metadata.flag_warning)
  
  # initialize
  cpu_list = []
  for cpu_id in cpu_ids:
    data[cpu_id] = {}
    # and build selector
    cpu_item = { 'id': cpu_id, 'value': cpu_id }
    if cpu_id == 'all':
      cpu_item['label'] = cpu_id
    else:
      cpu_item['label'] = 'CPU ' + str(cpu_id)
      # check if warning or alert
      if cpu_id in host_metadata.flag_alert:
        cpu_item['type'] = 'alert'
      elif cpu_id in host_metadata.flag_warning:
        cpu_item['type'] = 'warning'
    cpu_list.append(cpu_item)  
    for stat in stats:
      data[cpu_id][stat] = []

  # go through bucket in sorted order, inclusive of all buckets
  for i in range(min(buckets),max(buckets)+1):
      
    # add timestamp  
    xAxis.append(report_context.bucket_id_to_timestamp(i).strftime(JSON_DATE_FMT))
    # if bucket does not exist, add empty points
    if i not in buckets or hostname not in buckets[i]:
      add_empty_point(data, None )

    # go through each cpu of interest
    else:
      
      for cpu_id in cpu_ids:
        if cpu_id not in buckets[i][hostname]:
          add_empty_point(data, None )
        else:  
          # now go through list of stats
          # ensure we have all datapoints corresponding to xAxis
          for stat in stats:
            if stat in buckets[i][hostname][cpu_id] and buckets[i][hostname][cpu_id][stat] != None:
              # chart multiplies by 100 for percentage, so we divide here  
              data[cpu_id][stat].append(buckets[i][hostname][cpu_id][stat]/100.0)
            else:
              data[cpu_id][stat].append( None )

  # add empty buckets
  add_start_end_times(report_context,
                       buckets,
                       xAxis,
                       data)

  # now build series that we will bind to UI object
  # keyed by the cpu_id
  series = {}
  for cpu_id in cpu_ids:
    series[cpu_id] = []
    for stat in stats:
      if stat in data[cpu_id]:
        seriesItem = { 'id': stat,
                       'name': _get_stat_label(stat),
                       'items': data[cpu_id][stat] }
        # set color for consistency with CPU Utilization chart
        if stat == USR or stat == SYS or stat == WIO:
          seriesItem['color'] = _get_stat_color(stat)
        # hide idle by default, otherwise, it will always be 100%  
        elif stat == IDL:
          seriesItem['visibility'] = 'hidden'
        series[cpu_id].append(seriesItem)


  # return data to caller
  return (xAxis, series, cpu_list)

#------------------------------------------------------------
def print_charts(filelist, report_context):
  '''
    main driver - either called from main or from other python modules
    (e.g. exawchart.py - wrapper for generating all charts)
  '''
  
  # first parse the files
  exawparse_mp.parse_input_files(filelist, report_context)

  # get metadata with host information
  metadata = exawparse_mp.hostnames
  
  # print charts if we processed something
  for hostname in metadata:
    # get chart with timeseries data, average across all cpus
    (xAxis, series, cpu_list) = _print_all_chart(report_context,
                                                 exawparse_mp.buckets,
                                                 metadata[hostname])
    
    # get chart with average usage per cpu, no time series
    (cpuIds, cpuIdsSeries) = _print_cpu_id_chart(report_context,
                                                 report_context.hostnames[hostname].mpstat.summary_stats,
                                                 metadata[hostname])    
    # convert to JSON
    xAxisJson = json.dumps(xAxis)
    seriesJson = json.dumps(series)
    cpuListJson = json.dumps(cpu_list)
    cpuIdsJson = json.dumps(cpuIds)
    cpuSeriesJson = json.dumps(cpuIdsSeries)

    # get report context
    report_context_obj = report_context.get_json_object()
    report_context_obj['host'] = hostname
    report_context_obj['processedFiles'] = metadata[hostname].processed_files
    reportContextJson = json.dumps(report_context_obj)

    # now write out html file
    try:
      template_file = open(os.path.join(report_context.template_dir,
                                        'mpstat_template.html'), 'r')
      template = template_file.read()
      template_file.close()

      (filename, title) = report_context.write_html_file(
                            hostname + '_mp.html',
                            'CPU Detail',
                            template % vars())
      report_context.add_html_file(hostname, 'mpstat', (filename, title))
    except Exception as e:
      report_context.log_msg('error','Unable to read template file: %s (%s)' %
                             (os.path.join(report_context.template_dir,
                                          'mpstat_template.html'), str(e)))


#------------------------------------------------------------
def process_host_mpstat_summary(report_context,host):
  '''
    returns json object with summary information for mpstat.
    This will be used to populate cell summary page
    Note: we do this here, so we can process all mpstat data
    together, including analysis of mpstat data
  '''
  series_data = []
  findings = []
  if host in report_context.hostnames and 'all' in report_context.hostnames[host].mpstat.summary_stats:
    mpstat_summary = report_context.hostnames[host].mpstat.summary_stats['all']
    findings = report_context.hostnames[host].mpstat.findings
    report_context.log_msg('debug','mpstat: %s' % mpstat_summary)
    for stat in [ USR, NICE, SYS, WIO, 'irq', 'soft', STL, 'guest' ]:
      if mpstat_summary[stat] > 0:
        series_item = { 'name': _get_stat_label(stat),
                              'items': [ float(mpstat_summary[stat]/100) ] }
        if stat in [ USR, SYS, WIO ]:
          series_item['color'] = _get_stat_color(stat)
        series_data.append(series_item)

    # also display findings
    report_context.log_msg('debug','mpstat findings: %s' % report_context.hostnames[host].mpstat.findings)

  return  { 'groups': [ 'avg' ],
            'seriesData': series_data,
            'findings': findings,
            'htmlFiles' : report_context.hostnames[host].mpstat.html_files} 

#------------------------------------------------------------
def usage():

  print '------------------------------------------------------------'
  print 'Usage: '
  print '  ' + sys.argv[0] + ' -z <list of files> -f <from_time> -t <to_time> [-o <output_directory>]'
  print
  print '  -z|--zfile: space-separated list of files '
  print '              if using multiple files, enclose the list in ""'
  print '  -f|--from: start_time in the following format'
  print '             ' + DATE_FMT_INPUT
  print '  -t|--to: end in the following format'
  print '             ' + DATE_FMT_INPUT
  print '  -o|--outdir: directory to put datafiles and png files'
  print '                         DEFAULT: current directory'
  print
  print '------------------------------------------------------------'
    
    
#------------------------------------------------------------
def main():

  _my_report_context = ReportContext()

    # process arguments
  try:
    opts, args = getopt.getopt(sys.argv[1:],
                               'z:f:t:o:x:m:g:h',
                               ['zfile=',
                                'from=', 'to=',
                                'outdir=', 
                                'max_buckets=',
                                'mask=','log=',
                                'help'] )
  except getopt.GetoptError as err:
    _my_report_context.log_msg('error', str(err), 2)
    usage()
    sys.exit(2)
  else:
    # initialize variables based on arguments passed in
    outdir = None                             # output directory
    filelist = []
    user_start_time = None
    user_end_time = None
    start_time = datetime.utcfromtimestamp(0)
    end_time   = datetime.utcfromtimestamp(0)
    max_buckets = DEFAULT_MAX_BUCKETS
    date_mask = DATE_FMT_INPUT    
    for o, a in opts:
      if o in ('-z', '--zfile'):
        # strip all whitespace before splitting into list
        filelist_tmp = re.sub(r'\s', ' ', a).split(' ')
        # now substitute wildcards
        for f in filelist_tmp:
          filelist += glob(f)
      elif o in ('-f', '--from'):
        user_start_time = a
      elif o in ('-t', '--to'):
        user_end_time = a
      elif o in ('-o', '--outdir'):
        outdir = a
      elif o in ('-x', '--max_buckets'):
        max_buckets = int(a)
      elif o in ('-m', '--mask'):
        date_mask = a
      elif o in ('-g', '--log'):
        _my_report_context.set_log_level(a.upper())
      elif o in ('-h', '--help'):
        usage()
        sys.exit()
      else:
        usage()
        _my_report_context.log_msg('error', 'Unrecognized option: ' + o)


  # set report context
  if len(filelist) == 0:
    _my_report_context.log_msg('error', 'Empty filelist', 2)
    sys.exit()

  # convert user start/end time based on mask
  try:
    start_time = datetime.strptime(user_start_time, date_mask)
    end_time = datetime.strptime(user_end_time, date_mask)
    _my_report_context.set_report_context(start_time = start_time,
                                          end_time = end_time,
                                          max_buckets = max_buckets,
                                          outdir = outdir)

    
  except ValueError as err:
    _my_report_context.log_msg('error','Invalid time: %s - %s (%s): %s' % (user_start_time, user_end_time, date_mask,str(err)),2)
    
  except Exception as err:
    _my_report_context.log_msg('error', 'Unable to set report context (%s)' % str(err))
  else:

    # now call main function to process the data and print charts
    print_charts(filelist,
                 _my_report_context)

    # display information as to what files were returned
    for host in sorted(_my_report_context.hostnames):
      _my_report_context.log_msg('info', '%s: generated files %s' % (host, _my_report_context.hostnames[host].mpstat.html_files))



#------------------------------------------------------------
# standard template
#------------------------------------------------------------
if __name__ == '__main__':
  main()
