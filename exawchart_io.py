#!/usr/bin/python

#
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
#
#     NAME
#       exawchart_io.py
#
#     DESCRIPTION
#       Creates charts for iostat data based on buckets produced by
#       exawparse_io
#       Produces a set of .html files (poor man's navigation for now)
#
#     MODIFIED   (MM/DD/YY)
#     cgervasi    12/14/16 - reference object: use low/high instead of min/max
#     cgervasi    09/28/16 - add summary page
#     cgervasi    09/23/16 - use exawutil add_empty_point
#     cgervasi    08/25/16 - add all points to x-Axis
#     cgervasi    08/15/16 - use template directory
#     cgervasi    08/12/16 - jet 2.0.1 requires jquery-ui 1-12-stable
#     cgervasi    08/03/16 - change name format
#     cgervasi    07/20/16 - move to JET
#     cgervasi    06/21/16 - fortify
#     cgervasi    05/16/16 - add support for multiple hosts
#     cgervasi    04/29/16 - use ChartWrapper
#     cgervasi    04/28/16 - add master slider
#     cgervasi    03/24/16 - Creation
#

#------------------------------------------------------------
# This module creates the html pages containing charts using iostat data.
# It can be called on its own (for debugging), or more typically,
# the print_charts() routine is called by exawchart.py
#
# This is expected to generate 3 html files per cell
# . IOStat Summary (aggregated/averaged information)
# . IOStat Detail (per disk information)
# . CPU Utilization
#
# If the files include multiple cells (based on the filenames)
# then this also generates multi-cell charts for
# . IOStat Summary
# . CPU Utilization
#
# The html files uses the JET CDN in order to display the charts.
#
# This calls routines in
# . exawparse_io.py - to generate buckets, the flash/disk list
#                     and the list of hostnames processed.
#                     exawparse_io.py is responsible for parsing
#                     the ExaWatcher generated Iostat files.
#------------------------------------------------------------

import getopt
import sys
import re
import os
import math

from datetime import datetime, timedelta
from glob import glob

import exawparse_io
import json
  
# import constants and common functions from exawutil
from exawutil import DATE_FMT_INPUT, CPU, FLASH, DISK, CNT, USR, NICE, SYS, WIO, STL, IDL, BUSY, RPS, WPS, RSECPS, WSECPS, AVGRQSZ, AVGQUSZ, AWAIT, SVCTM, UTIL, RMBPS, WMBPS, IOPS, MBPS, SUMMARY, AVG, DEFAULT_FLASH_DISKS, DEFAULT_HARD_DISKS, DEFAULT_MAX_BUCKETS, JSON_DATE_FMT, validate_disk_list,validate_disk, add_empty_point, add_start_end_times, ReportContext

# change json to only dump 6 decimal points for float
json.encoder.FLOAT_REPR = lambda o: format(o, '.6f')

#------------------------------------------------------------
def _get_label(stat_name):
  '''
    for a given statname (based on our internal representation in buckets)
    return the label to display in the chart.  This is used as the "name"
    property in the series.
  '''
  label = None
  if stat_name == IOPS:
    label = 'io/s'
  elif stat_name == RPS:
    label = 'r/s'
  elif stat_name == WPS:
    label = 'w/s'
  elif stat_name == MBPS:
    label =  'iomb/s'
  elif stat_name == RMBPS:
    label = 'rmb/s'
  elif stat_name == WMBPS:
    label = 'wmb/s'
  elif stat_name == SVCTM:
    label = 'service time'
  elif stat_name == AWAIT:
    label = 'wait time'
  elif stat_name == UTIL:
    label = '%util'
  elif stat_name == USR:
    label = '%usr'
  elif stat_name == SYS:
    label = '%sys'
  elif stat_name == WIO:
    label = '%wio'
    
  return label

#------------------------------------------------------------
def _print_cpu_chart(report_context,
                     buckets,
                     host_metadata):
  '''
    This creates the cpu utilization html page
    PARAMETERS:
      report_context : ReportContext to process, includes time range,
                       bucket interval, num_buckets
      buckets        : parsed results of cpu data, in buckets
      host_metadata  : HostMetadata object that has the information for
                       the host, including name and processed files
    The HTML file generated will be added to report_context.html_files
  '''
  # get hostname
  hostname = host_metadata.name

  # list of CPU stats to process
  stats = [ USR, SYS, WIO ]
  
  # initialize objects that will be converted into JSON and plugged into
  # the javascript code.
  # Format of data:
  # data : { USR: [ list of values ],
  #          SYS: [ list of values ],
  #          WIO: [ list of values ] }
  # Data will be the 'items' property of the series in the chart.
  xAxis = []    # list of xAxis times
  data = {}     
  for stat in stats:
    data[stat] = []

  # inclusive of last bucket
  for i in range(min(buckets),max(buckets)+1):

    xAxis.append(report_context.bucket_id_to_timestamp(i).strftime(JSON_DATE_FMT))

    if i not in buckets or hostname not in buckets[i] or CPU not in buckets[i][hostname]:
      add_empty_point(data, None )
      
    else:  
      for stat in stats:
        if stat in buckets[i][hostname][CPU]:
          # chart multiplies by 100 for percentage, so we divide it by 100 here
          data[stat].append(buckets[i][hostname][CPU][stat]/100.0)
        else:
          data[stat].append( None )
  
  # add empty buckets
  add_start_end_times(report_context,
                      buckets,
                      xAxis,
                      data)

  # construct object that will be dumped as JSON, this is the format required
  # by JET charts for the series
  xAxisJson = json.dumps(xAxis)
  series = [ { 'name': '%usr',
               'lineWidth': 1,
               'color': '#00CC00',
               'items': data[USR] },
             { 'name': '%sys',
               'lineWidth': 1,
               'color': '#D2691E',
               'items': data[SYS] },
             { 'name': '%wio',
               'lineWidth': 1,
               'color': '#0094E7',
               'items': data[WIO] } ]
  seriesJson = json.dumps(series)  

  # also get the report context, which is also used by the JET charts
  # to display additional information, e.g. host, start/end times, etc.
  report_context_obj = report_context.get_json_object()
  report_context_obj['host'] = hostname
  report_context_obj['processedFiles'] = host_metadata.processed_files
  reportContextJson = json.dumps(report_context_obj)

  # write the html file (substituting placeholders in CPU_TEMPLATE),
  # and add the (filename,title) tuple into report_context
  try:
    template_file = open(os.path.join(report_context.template_dir,
                                      'cpu_template.html'), 'r')
    template = template_file.read()
    template_file.close()
    
    (filename,title) = report_context.write_html_file(
                                       hostname + '_cpu.html',
                                       'CPU Utilization',
                                       template % vars() )
    report_context.add_html_file( hostname, 'iostat', (filename,title) )
  except Exception as e:
    report_context.log_msg('error','Unable to read template file: %s (%s)' %
                           (os.path.join(report_context.template_dir,
                                        'cpu_template.html'), str(e)))
    
#------------------------------------------------------------
def _print_summary_chart(report_context,
                         buckets,
                         host_metadata):

  '''
    This creates the iostat summary html page
    PARAMETERS:
      report_context : ReportContext to process, includes time range,
                       bucket interval, num_buckets
      buckets        : parsed result of iostat data, in buckets
      host_metadata  : HostMetadata object that has the information for
                       the host, and flash/hard disk list
  '''
  # get hostname
  hostname = host_metadata.name

  # initialize objects that will be converted into JSON and plugged into
  # the javascript code.
  # Format of data:
  # { FLASH: { IOPS: [], RPS: [], WPS: [], RMBPS: [], SVCTM: [], AWAIT: [] ..},
  #   DISK: {IOPS: [], RPS: [], WPS: [], RMBPS: [], SVCTM: [], AWAIT: [] ... }
  # }
  # Data will be the 'items' property of the series in the charts
  # Note: all series (including xAxis) will need to have the same number
  # of datapoints
  xAxis = []     # list of xAxis items (time series)
  data = {}      # for storing series data
  disktypes = [] # disktypes (FLASH, DISK) seen in buckets

  hasFlash = (len(host_metadata.flash_disks) > 0)
  hasHardDisk = (len(host_metadata.hard_disks) > 0)

  if hasFlash:
    disktypes.append(FLASH)

  if hasHardDisk:
    disktypes.append(DISK)

  # initialize data arrays based on data that we have
  stats = [ IOPS, RPS, WPS, MBPS, RMBPS, WMBPS, SVCTM, AWAIT, UTIL ]
  for disktype in [ FLASH, DISK ]:
    data[disktype] = {}
    for stat in stats:
      data[disktype][stat] = []

  # now start populating arrays; for all buckets
  for i in range(min(buckets),max(buckets)+1):
    # always add to x-Axis
    xAxis.append(report_context.bucket_id_to_timestamp(i).strftime(JSON_DATE_FMT))
    if i not in buckets or hostname not in buckets[i]:
      # add the empty points for all charts
      add_empty_point(data, None)
      
    else:
      bucket = buckets[i][hostname]

      for statgroup in disktypes:

        if statgroup in bucket:
          summary_bucket = bucket[statgroup][SUMMARY]

          for stat in stats:
            # chart multiplies by 100 for percentages, so we divide here
            if stat == UTIL:
              value = summary_bucket[stat]/100.0
            else:
              value = summary_bucket[stat]
            data[statgroup][stat].append(value)

        # if we expect the disktype, but do not have it in this particular
        # bucket add null so the series has all required datapoints correctly
        else:
          for stat in stats:
            data[statgroup][stat].append( None )

  # add the start/end datapoints if required
  add_start_end_times(report_context,
                      buckets,
                      xAxis,
                      data)

    
  # now convert to Json
  xAxisJson = json.dumps(xAxis)
  
  # structure that we want in javascript is grouped by chart so we can
  # directly bind the chart series to our javascript structure, e.g.:
  # series : seriesJson.FLASH.IOPS
  # series is an array of objects, with each object representing a series in
  # the chart.  The 'items' property of each series contains the data.
  # Technically, we can just dump out data and let javascript do the grouping
  # per chart (so we get a clean separation between model:view).  However,
  # that means more javascript code, and since we're inlining the javascript
  # code in the html, we want to minimize the js code.
  #
  # structure of series data:
  # series_data: { FLASH: { <chart1>: [ {series object}, ... ], 
  #                         <chart2>. [ {series object}, ... ], ... }
  #                DISK:  { <chart1>: [ {series object}, ... ],
  #                         <chart2>. [ {series object}, ... ], ... }  }
  series_data = {}
  # create expected structures - we always want FLASH/DISK
  for statgroup in [ FLASH, DISK ]:
    series_data[statgroup]  = {}
    for chart_type in [ IOPS, MBPS, SVCTM, UTIL ]:
      series_data[statgroup][chart_type] = []

      # sets the source data (i.e. series to show) for each chart;
      # each series is one source stat
      if chart_type == IOPS:
        stats = [ IOPS, RPS, WPS ]
      elif chart_type == MBPS:
        stats = [ MBPS, RMBPS, WMBPS ]
      elif chart_type == SVCTM:
        stats = [ SVCTM, AWAIT ]
      elif chart_type == UTIL:
        stats = [ UTIL ]

      # create the series object, used by JET chart
      for stat in stats:
        series_item = { 'id': stat,
                        'name' : _get_label(stat),
                        'lineWidth': 1 }
        # in case we have EF we may not have the statgroup/disktype in data
        # we need to make sure we still have the object that gets bound in
        # UI
        if statgroup in data and stat in data[statgroup]:
          series_item['items'] = data[statgroup][stat]
        else:
          series_item['items'] = None

        series_data[statgroup][chart_type].append(series_item)

  # convert to json
  seriesJson = json.dumps(series_data)

  # also dump out max capacity for the cell
  capacity = None
  if host_metadata.capacity != None:
    capacity = {}
    for dtype in host_metadata.capacity:
      capacity[dtype] = {}
      for stat in host_metadata.capacity[dtype]:
        capacity[dtype][stat] = host_metadata.get_cell_capacity(dtype, stat)
        
  capacityJson = json.dumps(capacity)

  # convert report context information to Json as well, which will be
  # displayed in the HTML page
  report_context_obj = report_context.get_json_object()
  report_context_obj['host'] = hostname
  report_context_obj['processedFiles'] = host_metadata.processed_files
  reportContextJson = json.dumps(report_context_obj)

  # dump out disk types
  diskTypesJson = json.dumps(disktypes)
  
  # write the html file (substituting placeholders in SUMMARY_TEMPLATE),
  # and add the (filename, title) tuple into report_context.
  try:
    template_file = open(os.path.join(report_context.template_dir,
                                      'iosummary_template.html'), 'r')
    template = template_file.read()
    template_file.close()
    (filename,title) = report_context.write_html_file(
                                       hostname + '_iosummary.html',
                                       'IOStat Summary',
                                       template % vars())
    report_context.add_html_file( hostname, 'iostat', (filename,title) )    

  except:
    report_context.log_msg('error','Unable to read template file: %s' %
                           os.path.join(report_context.template_dir,
                                        'iosummary_template.html'))

#------------------------------------------------------------
def _print_detail_charts(report_context,
                         buckets,
                         host_metadata,):
  '''
    This creates the iostat detail html page
    PARAMETERS:
      report_context : ReportContext to process, includes time range,
                       bucket interval, num_buckets
      buckets: dictionary object keyed by bucket id with data points for
               the chart; this is created by exawparse_io.parse_input_files
      host_metadata: HostMetadata object which includes hostname, list of
                     flash/hard disks and processed files

  '''
  # we want to group this based on how we will be using it in the chart
  # i.e. we have an xAxis for the timestamps
  #      then a structure which we can bind to the series items
  #
  # to easily bind data, we use the following where
  # series is bound to data.<disktype>.stat
  # data = { FLASH: IOPS: { disk1: [], disk2: [], .... },
  #                 MBPS: { disk1: [], disk2: [], .... },
  #                 ...
  #          DISK:  IOPS: { disk1: [], disk2: [], ... },
  #                 MBPS: { disk1: [], disk2: [], ... }
  #        }
  # 
  # We also keep track of lo/hi range, and the structure is similar:
  # lohi = { FLASH:
  #            IOPS: {[ {'low':x, 'high':y}, {'low':x, 'high':y},... ],
  #                   avg: [ x, y, z, ... ] }     
  #        }              
  # get host metadata information
  hostname = host_metadata.name
  hasFlash = (len(host_metadata.flash_disks) > 0)
  hasHardDisk = (len(host_metadata.hard_disks) > 0)

  disktypes = []   # disktypes seen in buckets

  # build disk types
  if hasFlash:
    disktypes.append(FLASH)
  if hasHardDisk:
    disktypes.append(DISK)

  # list of disks, per disktype
  disklist = { FLASH: host_metadata.flash_disks,
               DISK: host_metadata.hard_disks }

  # stats and charts are the same since the series is disk name
  stats = [ IOPS, MBPS, SVCTM, AWAIT, UTIL ]

  # initialize structures
  xAxis = []
  data = {}
  lohi = {}

  # initialize data arrays based on data we expect to have
  # i.e. not all disks may have a datapoint in each bucket
  for disktype in [ FLASH, DISK ]:
    data[disktype] = {}
    lohi[disktype] = {}
    for stat in stats:
      data[disktype][stat] = {}  
      data[disktype][stat]['avg'] = []  # also add series for avg
      lohi[disktype][stat] = []         # array of min/max items
      for disk in disklist[disktype]:
        data[disktype][stat][disk] = [] # initialize data for each disk

  # now go through buckets, again note that all series should have the
  # same number of datapoints as the xAxis
  for i in range(min(buckets),max(buckets)+1):
    # alwas append to xAxis
    xAxis.append(report_context.bucket_id_to_timestamp(i).strftime(JSON_DATE_FMT))

    # add empty points if needed
    if i not in buckets or hostname not in buckets[i]:
      add_empty_point(data, None)
      # we also need to add it to lohi
      for disktype in disktypes:
        for stat in stats:
          lohi[disktype][stat].append( { 'low': None, 'high': None })          

    else:  

      bucket = buckets[i][hostname]
  
      for disktype in disktypes:
        if disktype in bucket:
          for stat in stats:
            # calculate lo/hi/avg for each disktype/stat/bucket
            bucket_tmp = { 'lo': None, 'hi': None, 'sum': 0, CNT: 0 }
  
            for disk in disklist[disktype]:
              if disk in bucket[disktype]:
                if stat == UTIL:
                  value = bucket[disktype][disk][stat]/100
                else:
                  value = bucket[disktype][disk][stat]
                data[disktype][stat][disk].append(value)
                # calculate lo/hi
                if bucket_tmp['lo'] == None or value < bucket_tmp['lo']:
                  bucket_tmp['lo'] = value
                if bucket_tmp['hi'] == None or value > bucket_tmp['hi']:
                  bucket_tmp['hi'] = value
                bucket_tmp['sum'] += value
                bucket_tmp[CNT] += 1
  
              # we expect this disk for this disktype, but it is not in bucket
              # add empty points for the chart
              else:
                data[disktype][stat][disk].append( None )
  
            # after al disks processed for this bucket, calculate the average
            if bucket_tmp[CNT] > 0:
              bucket_tmp[AVG] = bucket_tmp['sum']/bucket_tmp[CNT]
            else:
              bucket_tmp[AVG] = None
            # and add low/high for this stat's datapoint
            lohi[disktype][stat].append( { 'low': bucket_tmp['lo'],
                                           'high': bucket_tmp['hi'] } );
            data[disktype][stat]['avg'].append(bucket_tmp[AVG])
  
        # we expect this disk type (either FLASH or HARD but it is not in the
        # bucket
        else:
          for stat in stats:
            # also add it to lohi, to make sure we get all datapoints
            # the range chart reference object in the charts uses low/high
            lohi[disktype][stat].append({ 'low': None, 'high': None })
            data[disktype][stat]['avg'].append( None )
            for disk in disklist[disktype]:
              data[disktype][stat][disk].append( None )
  

  # add empty datapoints for start/end, if needed
  add_start_end_times(report_context,
                      buckets,
                      xAxis,
                      data)

  # also add empty datapoints for lo/hi
  if 0 not in buckets:
    for disktype in disktypes:
      for stat in stats:
        lohi[disktype][stat].insert(0, { 'low': None, 'high': None })
  last_bucket_id = report_context.get_bucket_id(report_context.report_end_time)
  if last_bucket_id not in buckets:
    for disktype in disktypes:
      for stat in stats:
        lohi[disktype][stat].append( { 'low': None, 'high': None })

  # now start dumping out json information
  xAxisJson = json.dumps(xAxis)

  # for series we want the format:
  # series_data = { FLASH:
  #                  { IOPS: { name: <disk>, id: <disk>, lineWidth: 1,
  #                           items: [] } } ...
  #
  # Technically, we can just dump out data and let javascript do the grouping
  # per chart (so clean separation of model:view).  However, that means
  # more javascript code, and since we're inlining the javascript code
  # in the html, we want to minimize that code ...
  series_data = {}
  for disktype in [ FLASH, DISK ]:
    series_data[disktype] = {}
    for chart_type in stats:
      series_data[disktype][chart_type] = []
      # include avg bucket as a series
      for disk in disklist[disktype] + [ 'avg' ]:
        series_item = { 'id': disk,
                        'name': disk,
                        'lineWidth': 1 }
        if disktype in data and chart_type in data[disktype] and disk in data[disktype][chart_type]:
          series_item['items'] = data[disktype][chart_type][disk]
        else:
          series_item['items'] = None
        series_data[disktype][chart_type].append(series_item)

  seriesJson = json.dumps(series_data)

  # create data arrays with min/max information bound to items for
  # the reference object; note the rest of the reference object is
  # defined in js code, as that has mostly UI information
  seriesLoHiJson = json.dumps(lohi)

  # also dump out list of disks, to populate the disk selector in the UI
  disk_selector = { FLASH: [], DISK: [] }
  for disktype in disklist:
    for disk in ['avg','all' ] + sorted(disklist[disktype]):
      if disk == 'avg':
        disk_selector[disktype].append( {'value': disk, 'label': 'Average' } )
      elif disk == 'all':
        disk_selector[disktype].append( {'value': disk, 'label': 'All' } )
      else:
        disk_selector[disktype].append( {'value': disk, 'label': disk } )

  diskSelectorJson = json.dumps(disk_selector)

  # and dump out capacity
  capacityJson = json.dumps(host_metadata.capacity)
  
  # dump report context information displayed in the UI
  report_context_obj = report_context.get_json_object()
  report_context_obj['host'] = hostname
  report_context_obj['processedFiles'] = host_metadata.processed_files
  reportContextJson = json.dumps(report_context_obj)

  # dump out disk types
  diskTypesJson = json.dumps(disktypes)
  
  # write out html file, substituting placeholders in DETAIL_TEMPLATE,
  # add the (filename,title) tuple to report context
  try:
    template_file = open(os.path.join(report_context.template_dir,
                                      'iodetail_template.html'), 'r')
    template = template_file.read()
    template_file.close()

    (filename,title) =  report_context.write_html_file(
                                        hostname + '_iodetail.html',
                                        'IOStat Detail',
                                        template % vars())
    report_context.add_html_file( hostname, 'iostat', (filename, title) )
    
  except:
    report_context.log_msg('error', 'Unable to read template file: %s' %
                           os.path.join(report_context.template_dir,
                                      'iodetail_template.html'))

  

#------------------------------------------------------------
def _chart_multicell_summary(report_context,
                             buckets,
                             iostat_metadata):

  '''
    This creates the multicell IO summary page
    PARAMETERS:
      report_context : ReportContext to process, includes time range,
                       bucket interval, num_buckets
      buckets        : parsed result of iostat data, in buckets
      iostat_metadata: HostMetadataIostat object from parsing iostat
  '''
  # Required data structures for summary chart
  # data = { FLASH: { IOPS: { host1: [], ... hostn: [] },
  #                   MBPS: { host1: [], ... hostn: [] },
  #                  AWAIT: { host1: [], ... hostn: [] },
  #                  SVCTM: { host1: [], ... hostn: [] },
  #                   UTIL: { host1: [], ... hostn: [] },
  #                 },
  #          DISK: { IOPS: { host1: [], ... hostn: [] },
  #                ...
  #                }
  #        

  # check which disk types we have across all hosts
  disktypes = []
  hostnames = []  # maintain list of hostnames for easier processing
  for host in sorted(iostat_metadata):
    hostnames.append(host)
    if FLASH not in disktypes and len(iostat_metadata[host].flash_disks) > 0:
      disktypes.append(FLASH)
    if DISK not in disktypes and len(iostat_metadata[host].hard_disks) > 0:
      disktypes.append(DISK)

  # charts we will display, the series for each chart is a host
  stats = [ IOPS, MBPS, SVCTM, AWAIT, UTIL ]

  # initialize structures
  xAxis = [] 
  data = {}
  for disktype in [ FLASH, DISK ]:
    data[disktype] = {}
    for stat in stats:
      data[disktype][stat] = {}
      # initialize per host
      for host in hostnames:
        data[disktype][stat][host] = []


  # now go through buckets
  for i in range(min(buckets),max(buckets)+1):
    
    xAxis.append(report_context.bucket_id_to_timestamp(i).strftime(JSON_DATE_FMT))

    if i not in buckets:
      add_empty_point(data, None )

    else:
      # we need to make sure we have all the required data points 
      for disktype in disktypes:
        for stat in stats:
          for host in hostnames:
            if host in buckets[i] and disktype in buckets[i][host] and SUMMARY in buckets[i][host][disktype]:
              summary_bucket = buckets[i][host][disktype][SUMMARY]
              if stat == UTIL:
                data[disktype][stat][host].append(summary_bucket[stat]/100)
              else:
                data[disktype][stat][host].append(summary_bucket[stat])
            else:
              data[disktype][stat][host].append( None )
  
  # add empty start/end times if required
  add_start_end_times(report_context,
                      buckets,
                      xAxis,
                      data)
  
  # now build series Items for easy binding
  series_data = {}
  for disktype in [ FLASH, DISK ]:
    series_data[disktype] = {}
    for chart_type in stats:
      series_data[disktype][chart_type] = []
      for host in hostnames:
        host_short = host.split('.',1)[0]
        series_item = { 'id': host,
                        'name': host_short,
                        'lineWidth': 1 }
        if disktype in data and chart_type in data[disktype] and host in data[disktype][chart_type]:
          series_item['items'] = data[disktype][chart_type][host]
        else:
          series_item['items'] = None
        series_data[disktype][chart_type].append(series_item)  

  # dump out data for populating the selector with the hostnames
  selector = []
  for host in hostnames:
    host_short = host.split('.',1)[0]
    selector.append( {'value': host, 'label': host_short } )
    
  # now dump json structures
  xAxisJson = json.dumps(xAxis)
  seriesJson = json.dumps(series_data)
  selectorJson = json.dumps(selector)

  # dump out report context information, that will be displayed in the
  # html page
  report_context_obj = report_context.get_json_object()
  reportContextJson = json.dumps(report_context_obj)

  # dump out disk types
  diskTypesJson = json.dumps(disktypes)

  # write out html file, substituting placeholders in
  # MULTICELL_SUMMARY_TEMPLATE, and add the (filename,title) tuple to
  # report_context
  try:
    template_file = open(os.path.join(report_context.template_dir,
                                      'multicell_iosummary_template.html'),
                         'r')
    template = template_file.read()
    template_file.close()

    (filename,title) = report_context.write_html_file(
                                       'iosummary.html',
                                       'IO Summary',
                                       template  % vars())
    report_context.add_html_file('', 'iostat', (filename, title) )

  except:
    report_context.log_msg('error','Unable to read template file: %s' %
                           os.path.join(report_context.template_dir,
                                        'multicell_summary_template.html'))
    

#------------------------------------------------------------
def _chart_multicell_cpu(report_context,
                         buckets,
                         iostat_metadata):
  '''
    This creates the multicell cpu page, which just displays % busy
    PARAMETERS:
      report_context : ReportContext to process, includes time range,
                       bucket interval, num_buckets
      buckets        : parsed result of iostat data, in buckets
      iostat_metadata: HostMetadataIostat object from parsing iostat
  '''
  # Required data structure::
  # . each series is a hostname
  # so we create
  # data = { host1: [values ...],
  #          host2: [values ...]
  #        }
  # and bind this as the series names
  # we use %busy (i.e 100- %idle) as the cpu utilization for each host

  xAxis =  []
  data = {}

  #initialize data based on hosts
  for host in iostat_metadata:
    data[host] = []

  # inclusive of all buckets
  for i in range(min(buckets),max(buckets)+1):
  
    xAxis.append(report_context.bucket_id_to_timestamp(i).strftime(JSON_DATE_FMT))
    # if bucket does not exist, add nulls to all datapoints
    if i not in buckets:
      add_empty_point(data, None )
      
    else:  
      # need to make sure we have all required datapoints
      # chart multiplies by 100 to display percentage
      for host in iostat_metadata:
        if host in buckets[i] and CPU in buckets[i][host]:
          data[host].append(buckets[i][host][CPU][BUSY]/100)
        else:
          data[host].append( None )

  add_start_end_times(report_context,
                      buckets,
                      xAxis,
                      data)

  # create series items for easy binding in javascript
  seriesData = []
  for host in sorted(data):
    seriesData.append( { 'id': host,
                         'name': host.split('.',1)[0],
                         'lineWidth': 1,
                         'items': data[host] } )

  # create selector for hostnames
  selector = []
  for host in sorted(iostat_metadata):
    host_short = host.split('.',1)[0]
    selector.append( {'value': host, 'label': host_short } )

  # dump json data
  xAxisJson = json.dumps(xAxis)
  seriesJson = json.dumps(seriesData)
  selectorJson = json.dumps(selector)

  # dump out report context
  report_context_obj = report_context.get_json_object()
  reportContextJson = json.dumps(report_context_obj)

  # write out html file, substituting placeholders in MULTICELL_CPU_TEMPLATE,
  # and add the (filename,title) tuple to report_context
  try:
    template_file = open(os.path.join(report_context.template_dir,
                                      'multicell_cpu_template.html'), 'r')
    template = template_file.read()
    template_file.close()
    
    (filename,title) = report_context.write_html_file(
                                      'cpu.html',
                                      'CPU Utilization',
                                      template % vars())
    report_context.add_html_file('', 'iostat', (filename, title) )

  except:
    report_context.log_msg('error','Unable to read template file: %s' %
                           os.path.join(report_context.template_dir,
                                        'multicell_cpu_template.html'))  

#------------------------------------------------------------
def print_charts(filelist,
                 flash_disks_user,
                 hard_disks_user,
                 report_context):


  '''
    main driver - either called from main() or from other 
    python modules (e.g. exawchart.py - wrapper for generating all charts)
  '''

  #first parse the files
  exawparse_io.parse_input_files(filelist,
                                 report_context,
                                 flash_disks_user = flash_disks_user,
                                 hard_disks_user  = hard_disks_user)
  
  # extract HostMetadataIostat information
  iostat_metadata = exawparse_io.hostnames

  # and now print the charts ... but only if we actually processed something
  if len(exawparse_io.hostnames) > 0:

    # first get multihost summary, if we have data from multiple hosts
    if report_context.multihost:
        
      _chart_multicell_summary(report_context,
                               exawparse_io.buckets,
                               iostat_metadata)
      _chart_multicell_cpu(report_context,
                           exawparse_io.buckets,
                           iostat_metadata)
       
    # and then get chart for each host
    for hostname in exawparse_io.hostnames:
      _print_summary_chart(report_context,
                           exawparse_io.buckets,
                           iostat_metadata[hostname])


      _print_detail_charts(report_context,
                           exawparse_io.buckets,
                           iostat_metadata[hostname])

      _print_cpu_chart(report_context,
                       exawparse_io.buckets,
                       iostat_metadata[hostname])

#------------------------------------------------------------
def process_host_iostat_summary(report_context, host):
  '''
    returns json objects with summary information for iostat
    this will be used to populate the cell summary page
    Note: we have this here, so we can process all iostat
    data together, including analysis of iostat data
  '''

  # initialize return objects
  series = {}
  findings = []
  
  # format we want for the bar charts will be
  # series = { iops: [{ name: r/s, items: [ flash val, disk val ] },
  #                   { name: w/s, items: [ flash val, disk val ] } ],
  #            mbps: [ { name: rmbps, items [] ... } ]...
  #
  if host in report_context.hostnames:
    iostat_summary = report_context.hostnames[host].iostat.summary_stats
    findings = report_context.hostnames[host].iostat.findings
    data = {}

    disktypes = []
    # first create the items arrays
    for stat in [ RPS, WPS, RMBPS, WMBPS, SVCTM, AWAIT, UTIL ]:
      if stat not in data:
        data[stat] = []
      for disktype in iostat_summary:
        if SUMMARY in iostat_summary[disktype]:
          summary_item = iostat_summary[disktype][SUMMARY]

          # keep track of disks we have
          if disktype not in disktypes:
            disktypes.append(disktype)
          
          # we basically append flash then hard disk
          if stat == UTIL:
            data[stat].append(float(summary_item[stat]/100))
          elif stat == AWAIT:
            # calculate queue time instead
            data[stat].append(float(summary_item[stat]-summary_item[SVCTM]))
          else:
            data[stat].append( summary_item[stat] )
                               
    # now create the series items
    series[IOPS]  = [ { 'name': 'r/s',          'items': data[RPS] },
                      { 'name': 'w/s',          'items': data[WPS] } ]
    series[MBPS]  = [ { 'name': 'rmb/s',        'items': data[RMBPS] },
                      { 'name': 'wmb/s',        'items': data[WMBPS] } ]
    series[AWAIT] = [ { 'name': 'service time', 'items': data[SVCTM] },
                      { 'name': 'queue time',   'items': data[AWAIT] } ]

    series[UTIL]  = [ { 'name': '%util',        'items': data[UTIL] } ]


  return { 'groups' : disktypes,
           'seriesData': series,
           'findings'  : findings,
           'htmlFiles' : report_context.hostnames[host].iostat.html_files} 

  
#------------------------------------------------------------
def usage():

  #prep flash/disks lists for replacement
  rep = { ",":"", "'": "", "[": "", "]": ""}
  # use these three lines to do the replacement
  ftext=str(DEFAULT_FLASH_DISKS)
  dtext=str(DEFAULT_HARD_DISKS)
  rep = dict((re.escape(k), v) for k, v in rep.iteritems())
  pattern = re.compile("|".join(rep.keys()))
  ftext = pattern.sub(lambda m: rep[re.escape(m.group(0))], ftext)
  dtext = pattern.sub(lambda m: rep[re.escape(m.group(0))], ftext)

  print '------------------------------------------------------------'
  print 'Usage: '
  print '  ' + sys.argv[0] + ' -z <list of files> -f <from_time> -t <to_time> [-p <list of disks>] [-l <list of flash>] [-o <output_directory>]'
  print
  print '  -z|--zfile: space-separated list of files '
  print '              if using multiple files, enclose the list in ""'
  print '  -f|--from: start_time in the following format'
  print '             ' + DATE_FMT_INPUT
  print '  -t|--to: end in the following format'
  print '             ' + DATE_FMT_INPUT
  print '  -p|--physical: space separated list of hard disks'
  print '    e.g. -p "sda sdb sdc sdd sde sdf sdg sdh sdi sdj sdk sdl" '
  print '    if not specified, will use default: '
  print '    "' + dtext + '"'
  print '  -l|--flash: space separated list of flash disks'
  print '    e.g. -f "sdn sdo sdp sdq sdr sds sdt sdu stv sdw sdx sdy sdz sdaa sdab sdac"'
  print '    if not specified, will use default: '
  print '    "' + ftext + '"'
  print '  -o|--outdir: directory to put datafiles and png files'
  print '                         DEFAULT: current directory'
  print
  print 'NOTE: '
  print '  -p and -l only have to be specified if not using default values '
  print '  and this list is only used if we did not find it in the ExaWatcher'
  print '  iostat header'
  print '------------------------------------------------------------'


#------------------------------------------------------------
def main():

  # create report context first
  _my_report_context = ReportContext()
  
  # process arguments
  try:
    opts, args = getopt.getopt(sys.argv[1:],
                               'p:l:z:f:t:o:x:m:g:h',
                               ['physical=', 'flash=', 'zfile=',
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
    flash_disks_user = DEFAULT_FLASH_DISKS
    hard_disks_user = DEFAULT_HARD_DISKS
    user_start_time = None
    user_end_time = None
    # initialize start/end times to epoch
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
      elif o in ('-p', '--physical'):
        # empty string should be empty list
        if a == '':
          hard_disks_user = []
        else:
          # strip all whitespace before splitting into list
          hard_disks_user = re.sub(r'\s', ' ', a).split(' ')
      elif o in ('-l', '--flash'):
        # empty string should be empty list
        if a == '':
          flash_disks_user = []
        else:
          # strip all whitespace before splitting into list
          flash_disks_user = re.sub(r'\s', ' ', a).split(' ')
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

  if not validate_disk_list(flash_disks_user):
    _my_report_context.log_msg('error', 'Invalid flash disk list: %s '% str(flash_disks_user),2);
    sys.exit()

  if not validate_disk_list(hard_disks_user):
    _my_report_context.log_msg('error', 'Invalid hard disk list: %s '% str(hard_disks_user),2);
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

    # fortify: create a new list
    flash_disks_list = []
    hard_disks_list = []
    for disk in flash_disks_user:
      diskname = validate_disk(disk)
      if diskname != None:
        flash_disks_list.append(diskname)

    for disk in hard_disks_user:
      diskname = validate_disk(disk)
      if diskname != None:
        hard_disks_list.append(diskname)

    # now call main function to process the data and print charts
    print_charts(filelist,
                 flash_disks_list,
                 hard_disks_list,
                 _my_report_context)

    # display information as to what files were returned
    for host in sorted(_my_report_context.hostnames):
      _my_report_context.log_msg('info', '%s: generated files %s' % (host, _my_report_context.hostnames[host].iostat.html_files))
  
#
#------------------------------------------------------------
# standard template
#------------------------------------------------------------
if __name__ == '__main__':
  main()
