#!/usr/bin/python

#
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
#
#     NAME
#       exawchart_inc.py
#
#     DESCRIPTION
#       Creates charts for iostat data based on buckets produced by
#       exawparse_io
#       Produces a set of .html files (poor navigation for now)
#
#     MODIFIED   (MM/DD/YY)
#     cgervasi    09/29/16 - add summary page
#     cgervasi    08/25/16 - cellcli or dbmcli
#     cgervasi    08/15/16 - use template directory
#     cgervasi    08/12/16 - jet 2.0.1 requires jquery-ui 1-12-stable
#     cgervasi    08/03/16 - change name format
#     cgervasi    07/20/16 - move to JET
#     cgervasi    04/29/16 - use ChartWrapper
#     cgervasi    03/24/16 - Creation
#

#------------------------------------------------------------
# This module creates the html page for alert history
# It can be called on its own (for debugging), or more typically,
# the print_charts() routine is called by exawchart.py
#
# The html file uses the JET CDN in order to display the charts.
#
# This uses cellcli to list alerthistory, so it can only be executed
# when running against the cell; cannot be executed when running
# exawchart.py against extracted files
#------------------------------------------------------------

import getopt
import sys
import re
import os
import json
import distutils.spawn

from operator import itemgetter
from subprocess import Popen, PIPE
from lxml import etree

from datetime import datetime, timedelta
from glob import glob
from exawutil import DEFAULT_MAX_BUCKETS, DATE_FMT_INPUT, JSON_DATE_FMT, FINDING_TYPE_INFO, get_hostname, ReportContext, timedelta_get_seconds

import exawrules

# cellcli command - do we need full path or any other checks here?
CLI = 'cellcli'

# commands we execute in cellcli
COMMAND_TZ = "-xml -e list alerthistory limit 1"
COMMAND_BT = "-xml -e list alerthistory where beginTime > '%(start_time)s' and beginTime < '%(end_time)s'"
COMMAND_ET = "-xml -e list alerthistory where endTime > '%(start_time)s' and endTime < '%(end_time)s'"

SEVERITY_CRITICAL='critical'
SEVERITY_WARNING='warning'
SEVERITY_INFO='info'

# Addl information for findings, these are really warnings/info
ALERT_MSG_01='Alerts not retrieved, current host (%s) is processing files extracted from another host (%s)'
ALERT_MSG_02='Unable to retrieve alerts (%s)'

#------------------------------------------------------------
class CellcliError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)


#------------------------------------------------------------
def _get_cli(report_context):
  global CLI

  # check if cellcli exists
  if distutils.spawn.find_executable('cellcli'):
    CLI = 'cellcli'
  elif distutils.spawn.find_executable('dbmcli'):
    CLI = 'dbmcli'
  else:
    CLI = None
  
  
  
#------------------------------------------------------------
def _get_cellcli_xml(cmdarg, report_context):
  '''
    executes the command in cellcli
    cmdarg is the string to pass into cellcli which should include the
    -xml flag
  '''
  root_xml = 'unknown'
  try:
    p = Popen([ CLI, cmdarg], stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=False)
    output, err = p.communicate()
    rc = p.returncode

    if rc != 0:
      raise CellcliError(output + ' ' + err)

    output = output.lstrip()
    # check output
    if len(output.strip()) == 0:
      root_xml = None

    # return the XML
    root_xml = etree.fromstring(output)

  except CellcliError as e:
    report_context.log_msg('error', 'cellcli error: %s' % (e.value))
  except OSError as e:
    report_context.log_msg('warning', 'unable to execute cellcli: %s' % str(e))
  except Exception as e:
    report_context.log_msg('error', str(e))
  finally:
    return root_xml

#------------------------------------------------------------
def _get_tzinfo(report_context):
  '''
    gets timezone offset from the cells
    We need the tz offset to construct the 'list alerthistory' command
    correctly
  '''
  root_xml = _get_cellcli_xml(COMMAND_TZ, report_context)

  # nothing to do, no alerts
  if root_xml == None or root_xml == 'unknown':
    return root_xml

  # attempt to get the timezone from beginTime, as beginTime should always
  # be in the XML
  begin_time_list = root_xml.xpath('//beginTime')
  if len(begin_time_list) != 0:
    return (begin_time_list[0].text)[-6:]

  # if no begin time, then that means we have no alerts
  return None

#------------------------------------------------------------
def _get_incidents(report_context, hostname):
  '''
  executes cellcli list alerthistory with specified start/end times
  '''

  # first make sure we have report context object
  report_context.add_hostinfo(hostname)
  alert_summary = report_context.hostnames[hostname].alerts.summary_stats
  alert_summary[SEVERITY_INFO] = 0
  alert_summary[SEVERITY_WARNING] = 0
  alert_summary[SEVERITY_CRITICAL] = 0

  data = []

  # initialize list of alert names - we maintain this so we don't
  # get dups when querying based on beginTime followed by endTime
  alerts = []

  # in order to construct query properly, we attempt to get the timezone
  # using list alert history
  tz_offset = _get_tzinfo(report_context)

  # if no tz_offset, that means we don't have any thing to do
  if tz_offset == None:
    report_context.log_msg('info', 'No alerts')
    return data

  # if we can't figure out tz_offset, then we can't construct proper
  # list alerthistory in cellcli, so we bail
  if tz_offset == 'unknown':
    report_context.log_msg('warning', 'Not retrieving alerts - unable to determine tz offset')
    return data

  # otherwise construct start time/end time by appending tz offset
  start_time = report_context.report_start_time.strftime('%Y-%m-%dT%H:%M:%S') + tz_offset

  end_time = report_context.report_end_time.strftime('%Y-%m-%dT%H:%M:%S') + tz_offset

  # first get alerts where beginTime is in the specified range
  root = _get_cellcli_xml( COMMAND_BT % vars(), report_context)
  # then get alerts where the endTime is in the specified range
  root_et = _get_cellcli_xml( COMMAND_ET % vars(), report_context)

  if root == None and root_et == None:
    report_context.log_msg('info', 'No alerts')

  if root == 'unknown' or root_et == 'unknown':
    report_context.log_msg('warning', 'Unable to get all alert history')

  # append end time XML to begin time XML, so we only process one XML
  root.append(root_et)

  # parse the XML
  for ah in root.iter('alerthistory'):
    # build alert information
    alert_item = {}
    for attr in ah.iter():
      # convert beginTime/endTime to a datetime
      if (attr.tag == 'beginTime' or attr.tag == 'endTime') and attr.text != None:
          (ts, offset) = attr.text[0:-6], attr.text[-6:]
          offset = offset.replace(':','') # since %z does not use ':'
          alert_item[attr.tag] = datetime.strptime(ts,
                                                 '%Y-%m-%dT%H:%M:%S')
      # only add XML information where we actually have a value
      elif attr.text != None:
        alert_item[attr.tag] = attr.text

    # check if we need to add this alert
    # it has to be in the window we want (which should be the case most of the
    # the time) and we also eliminate dups, since we queried alert history
    # twice
    add_item = False
    if alert_item['beginTime'] >= report_context.report_start_time and alert_item['beginTime'] <= report_context.report_end_time and alert_item['name'] not in alerts:
      add_item = True
    elif 'endTime' in alert_item and alert_item['endTime'] >= report_context.report_start_time and alert_item['endTime'] <= report_context.report_end_time and alert_item['name'] not in alerts:
      add_item = True

    if add_item and 'beginTime' in alert_item:
      # create object that we can use directly in JSON, which should be
      # of the format:
      # { id: <id>, title: <title>, start: <start>, end: <end>,
      #   description: <description>,
      #   style: "border-color: Red" }
      # color is based on type of alert

      # first add the alert to alerts,  so we don't get it twice;
      # this is only for dedup purposes
      alerts.append(alert_item['name'])

      # start populating required information for timeline chart
      alert_start = datetime.strftime(alert_item['beginTime'],
                                      JSON_DATE_FMT);
      alert_range = datetime.strftime(alert_item['beginTime'],
                                      DATE_FMT_INPUT);
      alert_end = None
      if 'endTime' in alert_item:
        alert_end = datetime.strftime(alert_item['endTime'],
                                      JSON_DATE_FMT);
        alert_range += (' - %s' % datetime.strftime(alert_item['endTime'],
                                                    DATE_FMT_INPUT))
        
      # set the title
      alert_title = alert_item['name'] + ': '
      if 'alertDescription' in alert_item:
        alert_title += alert_item['alertDescription'];
      elif 'alertMessage' in alert_item:
        alert_title += alert_item['alertMessage'];
        
      alert_description = '%s|%s|%s' % (alert_range,
                                        alert_item['alertType'],
                                        alert_item['severity'])

      # create an alert item with the required properties that we can
      # use to populate chart.  
      alert = { 'id':    alert_item['name'],
                'title': alert_title,
                'start': alert_start,
                'description': alert_description }
      if alert_end != None:
        alert['end'] = alert_end

      # set color based on severity
      if alert_item['severity'] == SEVERITY_CRITICAL:
        alert['style'] = 'border-color: Red;'
      elif alert_item['severity'] == SEVERITY_WARNING:
        alert['style'] = 'border-color: Gold;'

      # update summary based on severity
      alert_summary[alert_item['severity']] += 1

      # add it to data
      data.append(alert)


  # return a list/array of alerts
  return data

#------------------------------------------------------------
def _get_scale(report_context):
  '''
    gets major/minor scale for control data for the chart
  '''
  report_range = timedelta_get_seconds(report_context.report_end_time -
                                       report_context.report_start_time)

  # exawatcher has 7 days ..
  # if over a day, show days in minor scale
  if report_range > 24*3600:
    minor_scale = 'days'
    major_scale = 'weeks'
    zoom_order = ['weeks','days','hours'];
  # 1hour - 24 hours
  elif report_range > 3600:
    minor_scale = 'hours'
    major_scale = 'days'
    zoom_order = ['days','hours','minutes']
  else:
    minor_scale = 'minutes'
    major_scale = 'hours'
    zoom_order = ['hours','minutes','seconds']

  return (minor_scale, major_scale, zoom_order)
  

#------------------------------------------------------------
def _print_incidents_chart(alert_list,
                           report_context):

  # create control data
  (minor_scale, major_scale, zoom_order) = _get_scale(report_context)
  
  control_data = {
      'startTime': datetime.strftime(report_context.report_start_time,
                                   JSON_DATE_FMT),
      'endTime': datetime.strftime(report_context.report_end_time,
                                 JSON_DATE_FMT),
      'minorScale': minor_scale,
      'majorScale': major_scale,
      'zoomOrder': zoom_order }
                   
  seriesJson = json.dumps(alert_list)
  controlJson = json.dumps(control_data)

  # always runs on current host
  hostname = get_hostname()
  
  # set variables used by HTML template
  report_context_obj = report_context.get_json_object()
  report_context_obj['host'] = hostname
  reportContextJson = json.dumps(report_context_obj)

  # generate HTML file, substituting placeholders in INCIDENT_TEMPLATE,
  # and add (filename,title) tuple to report_context
  try:
    template_file = open(os.path.join(report_context.template_dir,
                                      'inc_template.html'), 'r')
    template = template_file.read()
    template_file.close()
    
    (filename, title) = report_context.write_html_file(
                                        hostname + '_inc.html',
                                        'Alert History',
                                        template % vars())

    report_context.add_html_file(hostname, 'alerts', (filename,title))
  except:
    report_context.log_msg('error','Unable to read template file: %s' %
                           os.path.join(report_context.template_dir,
                                        'inc_template.html'))  

  
#------------------------------------------------------------
def print_charts(report_context):
  '''
    main driver - either called from main() or from other
    python modules (e.g. exawchart.py - wrapper for generating all charts)
  '''
  file_tuple = (None, None)

  hostname = get_hostname()
  
  # first determine if we should use cellcli or dbmcli
  _get_cli(report_context)

  # if we know what command to use, then attempt to get alerts
  if CLI != None:
    # get list of alerts,
    alert_list = _get_incidents(report_context, hostname)
    # process rules
    _process_rules(report_context, hostname)
  
    if len(alert_list) > 0:
      _print_incidents_chart(alert_list, report_context)

  else:
    report_context.hostnames[hostname].alerts.add_finding(ALERT_MSG_02 % ('No cli command'), FINDING_TYPE_INFO)
    report_context.log_msg('warning','Not retrieving alerts.  No cli command')

#------------------------------------------------------------
def _process_rules(report_context, current_hostname):

  # list of callbacks for rules
  RULES_ALERT_HISTORY = [ exawrules.rule_alert_01_count ]

  # only process for current host
  for rule in RULES_ALERT_HISTORY:
    rule(report_context.hostnames[current_hostname].alerts)

      

#------------------------------------------------------------
def usage():

  print '------------------------------------------------------------'
  print 'Usage: '
  print '  ' + sys.argv[0] + ' -f <from_time> -t <to_time> [-o <output_directory>] '
  print
  print '  -f|--from: start_time in the following format'
  print '             ' + DATE_FMT_INPUT
  print '  -t|--to: end in the following format'
  print '             ' + DATE_FMT_INPUT
  print '  -o|--outdir: directory to put datafiles and png files'
  print '                         DEFAULT: current directory'
  print '------------------------------------------------------------'

#------------------------------------------------------------
def add_finding_no_collection(report_context,current_hostname):
  '''
    for adding the finding that we are not collecting alert
    history since we are running on a different host
    current_hostname: hostname of local host
    file_hostname: hostname in files processed
  '''
  # add the finding to all hosts processed
  for host in report_context.hostnames:
    report_context.hostnames[host].alerts.add_finding(ALERT_MSG_01 % (current_hostname, host), FINDING_TYPE_INFO)

    report_context.log_msg('debug','Findings: %s ' % report_context.hostnames[host].alerts.findings)

#------------------------------------------------------------
def main():

  _my_report_context = ReportContext()

  try:
    opts, args = getopt.getopt(sys.argv[1:],
                               'f:t:o:x:m:g:h',
                               ['from=', 'to=',
                                'outdir=',
                                'max_buckets=',
                                'mask=', 'log=',
                                'help'] )
  except getopt.GetoptError as err:
    _my_report_context.log_msg('error', str(err), 2)
    usage()
    sys.exit(2)
  else:
    # initialize variables based on arguments passed in
    outdir = None                           # output directory
    user_start_time = None
    user_end_time = None
    # initialize start/end times to epoch
    start_time = datetime.utcfromtimestamp(0)
    end_time   = datetime.utcfromtimestamp(0)
    max_buckets = DEFAULT_MAX_BUCKETS
    date_mask = DATE_FMT_INPUT    
    for o, a in opts:
      if o in ('-f', '--from'):
        user_start_time = a
      elif o in ('-t', '--to'):
        user_end_time = a
      elif o in ('-o', '--outdir'):
        outdir = a
      elif o in ('-x', '--max_buckets'):
        max_buckets = int(a)
      elif o in ('-m', '--mask'):
        date_mask = a
      elif o in ('-g','--log'):
        _my_report_context.set_log_level(a.upper());
      elif o in ('-h', '--help'):
        usage()
        sys.exit()
      else:
        usage()
        _my_report_context.log_msg('error', 'Unrecognized option: ' + o)

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
    _my_report_context.log_msg('error', 'Unable to set report context (%s)' % (str(err)))

  else:
    # now call main function to process the data and print charts
    print_charts(_my_report_context)

    # dislay information as to what files were returned
    for host in sorted(_my_report_context.hostnames):
      if len(_my_report_context.hostnames[host].alerts.html_files) > 0:
        _my_report_context.log_msg('info', '%s: generated files %s' % (host, _my_report_context.hostnames[host].alerts.html_files))


#
#------------------------------------------------------------
# standard template
#------------------------------------------------------------
if __name__ == '__main__':
  main()
