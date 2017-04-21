#!/usr/bin/python

#
# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
#
#     NAME
#       exawchart.py
#
#     DESCRIPTION
#       Wrapper script to generate charts (iostat, cellsrvstat, incidents)
#       and create *_menu.html and *_index.html
#
#     MODIFIED   (MM/DD/YY)
#     cgervasi    10/30/16 - accessibility
#     cgervasi    09/28/16 - add summary page
#     cgervasi    09/15/16 - add mpstat
#     cgervasi    08/15/16 - use template directory
#     cgervasi    08/12/16 - jet 2.0.1 requires jquery-ui 1-12-stable
#     cgervasi    08/03/16 - change name format
#     cgervasi    07/22/16 - move to JET
#     cgervasi    06/21/16 - fortify
#     cgervasi    04/11/16 - Creation
#

#------------------------------------------------------------
# This is the main driver to generate the charts.
# This will call functions in
# . exawchart_io.py - generates IO stat charts.  This includes
#                     IO Summary, IO Details, and CPU Utilization.
#                     If there are multiple hosts, then this will
#                     also generate the multi-cell charts for
#                     IO and CPU
# . exawchart_cs.py - generates cell server charts
# . exawchart_inc.py - if we are getting charts on the actual host
#                     this will also call this script to generate
#                     the alert history.  Note: since we call
#                     cellcli to get alert history, this can only
#                     be done if we're running on the actual cell
# Each of the exawchart_* scripts will add the html files it generates
# into the report_context, so that this main driver can then create
# the menu
#
# Note: all exaw*.py scripts will only work on files generate by ExaWatcher.
# It expects a certain format for both contents (i.e. headers in
# ExaWatcher files), along with the filenames (i.e. *Iostat*, *CellSrvStat*),
# and to extract the hostname from the filename.
#------------------------------------------------------------

import getopt
import sys
import re
import os
from glob import glob
from datetime import datetime
import json
import exawchart_io
import exawchart_cs
import exawchart_mp
import exawchart_inc

# import constants and common functions from exawutil
from exawutil import DATE_FMT_INPUT, DEFAULT_HARD_DISKS, DEFAULT_FLASH_DISKS, DEFAULT_MAX_BUCKETS, get_hostname, get_hostname_from_filename, validate_disk_list, validate_disk, ReportContext

# change json to only dump 6 decimal points for float
json.encoder.FLOAT_REPR = lambda o: format(o, '.6f')

# template for index.html
HTML_INDEX_TEMPLATE = '''
<!-- Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.-->
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8"/>
    <title>ExaWatcher Charts</title>
    <meta name="description" contents="Index page for exawatcher charts"/>
  </head>
  <frameset cols="20%%,80%%">
    <frame id="menu" src="%(menu_file)s" name="menu" title="Menu Navigation"/>
    <frame id="chart" src="%(first_chart)s" name="chart" title="Charts"/>
  </frameset>
</html>
'''


    
#------------------------------------------------------------
def _process_summary_pages(report_context):
  '''
    creates summary page for the cell to display averages over report
    interval along with findings
  '''
  # process and add this per host
  for host in report_context.hostnames:
    # skip multicell information for now
    if report_context.multihost and host == '':
      continue

    alertSummary = { 'findings' :report_context.hostnames[host].alerts.findings,
                     'htmlFiles': report_context.hostnames[host].alerts.html_files }
    
    iostatSummary = exawchart_io.process_host_iostat_summary(report_context,host)
    mpstatSummary = exawchart_mp.process_host_mpstat_summary(report_context,host)

    csstatSummary = exawchart_cs.process_host_cellsrvstat_summary(report_context,host)

    # construct global summary object
    summaryJson = json.dumps( { 'alerts': alertSummary,
                                'iostat': iostatSummary,
                                'mpstat': mpstatSummary,
                                'cellsrvstat': csstatSummary } )

    report_context.log_msg('debug','summary: %s' % summaryJson)
    
    # report context information
    report_context_obj = report_context.get_json_object()
    report_context_obj['host'] = host
    reportContextJson = json.dumps(report_context_obj)

    try:
      template_file = open(os.path.join(report_context.template_dir,
                                        'cell_summary_template.html'),'r')
      template = template_file.read()
      template_file.close()

      (filename, title) = report_context.write_html_file(host + '.html',
                                                         'Summary',
                                                         template % vars());
      report_context.add_html_file(host, 'summary', (filename, title), 0)
    except Exception as e:
      report_context.log_msg('error', 'Unable to read template file: %s (%s)' %
                             (os.path.join(report_context.template_dir,
                                          'cell_summary_template.html'), str(e)))

#------------------------------------------------------------
def usage():
  '''
    display script usage
  '''
  
  #prep flash/disks lists for replacement
  rep = { ",":"", "'": "", "[": "", "]": ""}
  # use these three lines displpay default flash/hard disks as text
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

  # create report context
  report_context = ReportContext()

  # process arguments
  try:
    opts, args = getopt.getopt(sys.argv[1:],
                               'p:l:z:f:t:o:x:m:g:h',
                               ['physical=', 'flash=', 'zfile=',
                                'from=', 'to=',
                                'outdir=', 'name=',
                                'max_buckets=',
                                'mask=', 'log=',
                                'help'] )
  except getopt.GetoptError as err:
    report_context.log_msg('error', str(err), 2)
    usage()
    sys.exit(2)
  else:
    # initialize variables based on arguments passed in
    outdir = ''                             # output directory
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
         # now expand each item in case there are wildcards in list
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
      # max buckets for testing
      elif o in ('-x', '--max_buckets'):
        max_buckets = int(a)
      # undocumented date mask
      elif o in ('-m', '--mask'):
        date_mask = a
      elif o in ('-g', '--log'):
        # set log level, we don't bother checking for allowed values
        # as this should only be used for debugging
        report_context.set_log_level(a.upper())
      elif o in ('-h', '--help'):
        usage()
        sys.exit()
      else:
        usage()
        report_context.log_msg('error', 'Unrecognized option: ' + o)

  # check arguments
  if len(filelist) == 0:
    report_context.log_msg('error', 'Empty filelist', 2)
    sys.exit()

  filelist=sorted(filelist)

  # fortify - disk list should only be nvm* or sd*
  if not validate_disk_list(flash_disks_user):
    report_context.log_msg('error', 'Invalid flash disk list: %s '% str(flash_disks_user),2);
    sys.exit()

  if not validate_disk_list(hard_disks_user):
    report_context.log_msg('error', 'Invalid hard disk list: %s '% str(hard_disks_user),2);
    sys.exit()

  # set report context
  try:
    # convert user start/end time based on mask
    start_time = datetime.strptime(user_start_time, date_mask)
    end_time = datetime.strptime(user_end_time, date_mask)

    report_context.set_report_context(start_time = start_time,
                                      end_time = end_time,
                                      max_buckets = max_buckets,
                                      outdir = outdir)

  except ValueError as err:
    report_context.log_msg('error','Invalid time: %s - %s (%s): %s' % (user_start_time, user_end_time, date_mask,str(err)),2)

  except Exception as err:
    report_context.log_msg('error', 'Unable to set report context (%s)' % str(err))

  else:

    # as we call different functions to print charts, each one will add
    # to the html files that it generates to report_context.html_files

    # generate iostat charts
    iostat_files = [ s for s in filelist if 'Iostat' in s ]
    if len(iostat_files) > 0:
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

      exawchart_io.print_charts(sorted(iostat_files),
                                flash_disks_list,
                                hard_disks_list,
                                report_context)

    # generate mpstat charts
    mp_files = [ s for s in filelist if 'Mpstat' in s ]
    if len(mp_files) > 0:
      exawchart_mp.print_charts(sorted(mp_files),
                                report_context)

    # generate cell server charts
    cs_files = [ s for s in filelist if 'CellSrvStat' in s ]
    if len(cs_files) > 0:
      # report_context.log_msg('info', 'Files for cellsrvstat: %s' % cs_files)
      exawchart_cs.print_charts(sorted(cs_files),
                                report_context)
      
    # now get incidents, but only if we are running on the host which
    # matches the filenames we have processed.
    # in case we have not generated any files, then get the hostname
    # from the first file in the filelist
    hostname = get_hostname()
    if len(report_context.hostnames) > 0:
      hostname_in_filename = report_context.hostnames.keys()[0]
    else:
      hostname_in_filename = get_hostname_from_filename(filelist[0])

    if not(report_context.multihost) and hostname_in_filename == hostname:
      exawchart_inc.print_charts(report_context)
    else:
      exawchart_inc.add_finding_no_collection(report_context,
                                              hostname)
      report_context.log_msg('info', 'Not collecting alert history')

    # Before creating the menu and index files, make sure we actually
    # generated charts.
    if report_context.num_html_files() == 0:
      report_context.log_msg('warning', 'No charts produced')
      sys.exit(2)

    # get information for summary page
    _process_summary_pages(report_context)
    
    # find the first chart so that it is displayed by the index page
    first_chart = report_context.hostnames[sorted(report_context.hostnames)[0]].get_first_chart()

    # get list of files in JSON format, plugged into javascript code of
    # menu template
    filesJson = json.dumps(report_context.get_html_files())
    reportContextJson = json.dumps(report_context.get_json_object())

    # create menu page
    if report_context.multihost:
      menu_suffix = 'menu.html'

    # include hostname if single-host, so we do not overwrite when
    # extracting tar'd file from multiple GetExaWatcherResults.sh
    else:
      # html_files is keyed by hostname, so we get it from there ...
      # as we know we've already generated files if we're here
      hname = report_context.hostnames.keys()[0]
      menu_suffix = hname + '_menu.html'

    try:
      template_file = open(os.path.join(report_context.template_dir,
                                        'menu_template.html'),'r')
      template = template_file.read()
      template_file.close()
      
      (menu_file,menu_title) = report_context.write_html_file(
                                 menu_suffix,
                                 'ExaWatcher Charts Menu',
                                 template % vars())
    except:
      report_context.log_msg('error','Unable to read template file: %s' %
                             os.path.join(report_context.template_dir,
                                          'menu_template'))
      
    # finally create index page (main) page
    else:
      report_context.write_html_file('index.html',
                                     'ExaWatcher Charts Main',
                                     HTML_INDEX_TEMPLATE % vars())

#
#------------------------------------------------------------
# standard template
#------------------------------------------------------------
if __name__ == '__main__':
  main()
