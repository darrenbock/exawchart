#!/usr/bin/python

#
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
#
#     NAME
#       exawchart_cs.py
#
#     DESCRIPTION
#       Creates charts for cellsrvstat data based on buckets produced by
#       exawparse_cs
#       Produces a set of .html files (poor man's navigation for now)
#
#     MODIFIED   (MM/DD/YY)
#     cgervasi    09/29/16 - add summary page
#     cgervasi    09/27/16 - factorize add_start_end_times
#     cgervasi    08/25/16 - add all points to x-Axis
#     cgervasi    08/15/16 - use template directory
#     cgervasi    08/12/16 - jet 2.0.1 requires jquery-ui 1-12-stable
#     cgervasi    08/03/16 - change name format
#     cgervasi    07/20/16 - move to JET
#     cgervasi    05/17/16 - add support for multiple hosts
#     cgervasi    04/29/16 - use ChartWrapper
#     cgervasi    04/28/16 - add master slider
#     cgervasi    04/05/16 - Creation
#

#------------------------------------------------------------
# This module creates the html pages containing charts using cellsrvstat data.
# It can be called on its own (for debugging), or more typically,
# the print_charts() routine is called by exawchart.py
#
# This is expected to generate an html file per cell
#
# The html files uses the JET CDN in order to display the charts.
#
# This calls routines in
# . exawparse_cs.py - to generate buckets, with the data points.
#                     exawparse_cs.py is responsible for parsing
#                     the ExaWatcher generated CellSrvStat files.
#------------------------------------------------------------

import getopt
import sys
import re
import os
import json

from glob import glob
from datetime import datetime

import exawparse_cs
# import common keys and metadata from exawparse_cs into our namespace
from exawparse_cs import METRIC_METADATA, METRIC_TYPE, METRIC_LIST, METRIC_DELTA, KEY, DISP_UNIT, CHART_GROUP, CHART_GROUP_IDS

# import constants and common functions frome exawutil
from exawutil import DATE_FMT_INPUT, VALUE, CNT, DEFAULT_MAX_BUCKETS, TITLE, JSON_DATE_FMT, add_start_end_times, ReportContext

# change json to only dump 6 decimal points for float
json.encoder.FLOAT_REPR = lambda o: format(o, '.6f')

#------------------------------------------------------------
# globals - this is either set by caller, or created if called using main()
_my_report_context = None

#------------------------------------------------------------
def _build_chart_map():
  '''
    builds the map of charts and labels based on METRIC_METADATA
    this returns the structure
    chart_map = { <key> : { TITLE: <chart title>,
                            'metrics': { gkey_mkey : { metric_metadata }
                                         ...
                                       }
                          }
                }                           
    where <key> is the chart key - either the group key (for chart groups)
    or the gkey_mkey for single series charts
    Note: gkey_mkey is repeated in 'metrics' as the key for single-series
    charts
  '''

  # initialize map
  chart_map = {}

  for (group, group_metadata) in METRIC_METADATA.iteritems():
    gkey = group_metadata[KEY]
    for (metric, metric_metadata) in group_metadata[METRIC_LIST].iteritems():
      mkey = metric_metadata[KEY]
      metric_label =  metric
      if TITLE in metric_metadata:
        metric_label = metric_metadata[TITLE]
      else:
        # note: we're modifying metric_metadata here ...
        # may not be such a good idea ... 
        metric_metadata[TITLE] = metric
      key = exawparse_cs.generate_key(gkey,mkey)
      if CHART_GROUP in metric_metadata:
        chart_group = metric_metadata[CHART_GROUP]
        if chart_group not in chart_map:
          chart_map[chart_group] = {
            TITLE: CHART_GROUP_IDS[metric_metadata[CHART_GROUP]],
            'metrics': {}  }
        chart_map[chart_group]['metrics'][key] = metric_metadata

      else:
        chart_map[key] = { TITLE: metric_label,
                           'metrics': { key: metric_metadata } }
  return chart_map

#------------------------------------------------------------
def _is_zero(key, chart_map, check_zero):
  '''
    for a given key, determines if all values are 0 by using the
    check_zero structure from the parsing routine.
    we do this to suppress charts/series that are all 0
  '''
  # go through list of metrics for chart
  chart_total = 0
  chart_check_zero = False  # chart has check_zero flag indicated
  for mkey in chart_map[key]['metrics']:
    if mkey in check_zero:
      chart_check_zero = True
      chart_total += check_zero[mkey]

  return (chart_total == 0 and chart_check_zero)

#------------------------------------------------------------
def _is_no_data(key, chart_map, data):
  '''
    for a given key, check if we actually have datapoints in data
  '''
  no_data = True
  for mkey in chart_map[key]['metrics']:
    if mkey in data:
      no_data = False

  return no_data

#------------------------------------------------------------
def _get_disp_unit(key,metric_metadata_list):
  '''
    returns display unit for the metric based on metadata
    if this is a delta metric, we add '/s' to the display unit
  '''
  disp_units = []
  for mkey in metric_metadata_list:
    disp_unit = ''
    if DISP_UNIT in metric_metadata_list[mkey]:
      disp_unit = metric_metadata_list[mkey][DISP_UNIT]
    if metric_metadata_list[mkey][METRIC_TYPE] == METRIC_DELTA:
      disp_unit += '/s'
    if disp_unit not in disp_units:
      disp_units.append(disp_unit)

  # check we only have a single unit for chart groups
  if len(disp_units) > 1:
    _my_report_context.log_msg('warning','chart %s has multiple units' % key)

  if len(disp_units) == 0:
    disp_unit = ''
  else:
    disp_unit = disp_units[0]
    
  return disp_unit


#------------------------------------------------------------
def _print_cellsrv_charts(buckets,
                          host_metadata,
                          report_context):

  '''
    This creates the cellsrvstat html page

    PARAMETERS:
      buckets: dictionary object keyed by bucket id with data points for
               the chart; this is created by exawparse_cs.parse_input_files
      host_metadata: metadata about host, including name, processed_files,
               metric_keys
      report_context: ReportContext object         

  '''

  # extract hostname
  hostname = host_metadata.name

  # for JET, we need 
  # . items array
  # . HTML section

  # data should simply convert the buckets dictionary objects into
  # a list, with each series in its own array
  # data = { <metric_key>: [ .... ] }
  # need to initialize with the list of metrics first
  data = {}
  xAxis = []
  
  # initialize data with all the keys
  for key in host_metadata.metric_keys:
    data[key] = []
  
  for i in range(min(buckets),max(buckets)+1):
    # add timestamp to xAxis
    xAxis.append(report_context.bucket_id_to_timestamp(i).strftime(JSON_DATE_FMT))
    if i not in buckets or hostname not in buckets[i]:
      for key in data:
        data[key].append( None )

    else:
      # go through all expected keys
      for key in host_metadata.metric_keys:
        if key in buckets[i][hostname]:
          data[key].append(buckets[i][hostname][key][VALUE])
        else:
          data[key].append( None )

  # add empty data points
  add_start_end_times(report_context,
                      buckets,
                      xAxis,
                      data)

  # get map and labels so we can easily build the strings for javascript
  chart_map= _build_chart_map()

  # create series items based on chart map
  suppressed_charts = [] # charts that are not displayed, all 0 values
  suppressed_series = [] # series that are not displayed
  no_data_charts    = [] # charts that are not displayed, no data
  series_data = {}
  chart_metadata = {}    # metadata for charts to be used by js code
  chart_order = []       # we want to maintain order in chart based on ids
  for key in sorted(chart_map):

    # first check if all metrics are 0
    if _is_zero(key, chart_map, host_metadata.check_zero):
      suppressed_charts.append(chart_map[key][TITLE])
      continue

    # check if we have data
    if _is_no_data(key, chart_map, data):
      no_data_charts.append(chart_map[key][TITLE])
      continue

    # check for the metrics
    chart_title = chart_map[key][TITLE]
    series_data[key] = []
    # also determine converter used based on display unit
    chart_metadata[key] = { TITLE: chart_title,
                            DISP_UNIT: _get_disp_unit(key, chart_map[key]['metrics']) }
    chart_order.append(key)
    
    for mkey in sorted(chart_map[key]['metrics']):
      series_title = mkey
      if TITLE in chart_map[key]['metrics'][mkey]:
        series_title = chart_map[key]['metrics'][mkey][TITLE]
      if mkey in data:
        # check if individual series is all 0
        if mkey in host_metadata.check_zero and host_metadata.check_zero[mkey] == 0:
          suppressed_series.append(series_title)
        else:
          series_data[key].append({ 'id': mkey,
                                            'name': series_title,
                                            'items': data[mkey],
                                            'lineWidth': 1 })
      else:
        suppressed_series.append(series_title)

  # convert to Json
  xAxisJson = json.dumps(xAxis)

  seriesDataJson = json.dumps(series_data)

  # note: chartMetadata also determines the charts that will be displayed
  chartMetadataJson = json.dumps(chart_metadata)
  chartOrderJson = json.dumps(chart_order)
  
  # get info from report context
  report_context_obj = report_context.get_json_object()
  report_context_obj['host'] = hostname
  report_context_obj['processedFiles'] = host_metadata.processed_files

  # now check for additional information, i.e. suppressed or all 0
  # charts/series
  report_context_obj['addlInfo'] = 'Additional information: '
  if len(suppressed_charts) > 0:
    report_context_obj['addlInfo'] += '<br/>The following charts are not displayed (all 0): %s<br/>The following stats are all 0 (or no data): %s<br/>No data for the following charts: %s' % (str(suppressed_charts), str(suppressed_series), str(no_data_charts))

  reportContextJson = json.dumps(report_context_obj)

  # generate the html file, substituting placeholders in CELLSRV_TEMPLATE,
  # and add the (filename,title) tuple to report context.
  try:
    template_file = open(os.path.join(report_context.template_dir,
                                      'cellsrv_template.html'), 'r')
    template = template_file.read()
    template_file.close()
  
    (filename, title) =  report_context.write_html_file(
                                         hostname + '_cellsrv.html',
                                         'CellSrvStat',
                                         template % vars())
    report_context.add_html_file(hostname, 'cellsrvstat', (filename,title) )
  except:
    report_context.log_msg('error','Unable to read template file: %s' %
                           os.path.join(report_context.template_dir,
                                        'cellsrv_template.html'))
    

#------------------------------------------------------------
def print_charts(filelist,
                 report_context):

  '''
    main driver - called from main() or from other python modules, e.g.
    exawchart.py wrapper script.
    PARAMETERS:
      filelist: list of files to process
      report_context: object with report context information, e.g
                      report start/end times, bucket interval, etc.
  '''

  global _my_report_context
  _my_report_context = report_context

  # first parse the files
  exawparse_cs.parse_input_files(filelist, _my_report_context)

  cellsrvstat_metadata = exawparse_cs.hostnames

  # generate the html file only if we processed files
  if len(cellsrvstat_metadata) > 0:
    # TODO: multi-cell processing here, once we decide which stats to
    # include for multicells

    # and then get info per cell
    for hostname in cellsrvstat_metadata:
      _print_cellsrv_charts(exawparse_cs.buckets,
                           cellsrvstat_metadata[hostname],
                           report_context)

#------------------------------------------------------------
def process_host_cellsrvstat_summary(report_context, host):
  # for the summary page, we only display Fc and Smart IO charts
  # this is hand-cobbled, not metadata driven as we need high-level
  # information
  # for FC - get all read + write requests / bytes
  # we want to group it such that each metric is a series
  # and the groups are Reads and Writes
  # the series for Reads and Writes will be different though
  #
  #
  cs_summary = report_context.hostnames[host].cellsrvstat.summary_stats
  chart_groups = { 'a01': [], 'a03': [], 'a07': [], 'a08': [], 'a09': [] }
  for g in METRIC_METADATA:
    gkey = METRIC_METADATA[g][KEY]
    # try to at least have reproducible order of metrics within each group
    for m in sorted(METRIC_METADATA[g][METRIC_LIST]):
      mkey = METRIC_METADATA[g][METRIC_LIST][m][KEY]
      if CHART_GROUP in METRIC_METADATA[g][METRIC_LIST][m] and METRIC_METADATA[g][METRIC_LIST][m][CHART_GROUP] in chart_groups:
        chart = METRIC_METADATA[g][METRIC_LIST][m][CHART_GROUP] 
        key = exawparse_cs.generate_key(gkey, mkey)
        if key in cs_summary:
          title = m
          if TITLE in METRIC_METADATA[g][METRIC_LIST][m]:
            title = METRIC_METADATA[g][METRIC_LIST][m][TITLE]
          chart_groups[chart].append( { KEY: key, TITLE: title,
                                        VALUE: cs_summary[key][VALUE] } )

  # now convert the information into a form consumable by the charts
  series = {}
  for g in chart_groups:
    if g == 'a01':
      chart_key = 'fcrrq'
    elif g == 'a03':
      chart_key = 'fcwrq'
    elif g == 'a07':
      chart_key = 'fcsz'
    elif g == 'a08' or g == 'a09':
      chart_key = 'sio'
    series[chart_key] = []
    for s in chart_groups[g]:
      # add label for this so it shows up in charts
      series[chart_key].append( { 'name': s[TITLE], 'items': [ { 'y': s[VALUE], 'label': s[VALUE] } ] } )

  return {'groups': ['avg'],
          'seriesData': series,
          'findings'  : report_context.hostnames[host].cellsrvstat.findings,
          'htmlFiles' : report_context.hostnames[host].cellsrvstat.html_files}
  

#------------------------------------------------------------
def usage():

  print '------------------------------------------------------------'
  print 'Usage: '
  print '  ' + sys.argv[0] + ' -z <list of files> -f <from_time> -t <to_time> [[-o <output_directory>]'
  print
  print '  -z|--zfile: space-separated list of files '
  print '              if using multiple files, enclose the list in ""'
  print '  -f|--from: start_time in the following format'
  print '             ' + DATE_FMT_INPUT
  print '  -t|--to: end in the following format'
  print '             ' + DATE_FMT_INPUT
  print '  -o|--outdir: directory to put datafiles and png files'
  print '                         DEFAULT: current directory'
  print '------------------------------------------------------------'


#------------------------------------------------------------
def main():

  _my_report_context = ReportContext()

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
    # initialize start/end times to epoch
    start_time = datetime.utcfromtimestamp(0)
    end_time   = datetime.utcfromtimestamp(0)
    max_buckets = DEFAULT_MAX_BUCKETS
    date_mask = DATE_FMT_INPUT    
    for o, a in opts:
      if o in ('-z', '--zfile'):
        # strip all whitespace before splitting into list
          filelist_tmp = re.sub(r'\s', ' ', a).split(' ')
          # now expand each item in case there are wildcards in list
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
    # print charts
    print_charts(filelist,
                 _my_report_context)
    
    # dislay information as to what files were returned
    for host in sorted(_my_report_context.hostnames):
      _my_report_context.log_msg('info', '%s: generated files %s' % (host, _my_report_context.hostnames[host].cellsrvstat.html_files))

#
#------------------------------------------------------------
# standard template
#------------------------------------------------------------
if __name__ == '__main__':
  main()
