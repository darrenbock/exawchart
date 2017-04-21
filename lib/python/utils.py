import datetime
import fileinput
import os
import cPickle as pickle
import pwd
import random
import re
import signal
import subprocess
import sys
import time

# get Exadata environmentals
env_source = 'source /opt/oracle.cellos/exadata.img.env'
env_dump = '/usr/bin/python -c "import os,pickle;print pickle.dumps(os.environ)"'
proc_env = os.popen('%s && %s' %(env_source, env_dump))
exadata_env = pickle.loads(proc_env.read())
os.environ = exadata_env

# OS command definitions
cellcli_cmd = 'cellcli'
chkconfig_cmd = '/sbin/chkconfig'
e2label_cmd = '/sbin/e2label'
file_cmd = '/usr/bin/file'
imageinfo_cmd = '/usr/local/bin/imageinfo'
pstree_cmd = '/usr/bin/pstree'
runlevel_cmd = '/sbin/runlevel'
service_cmd = '/sbin/service'
uname_cmd = '/bin/uname'

# Globals
datestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M.%S')
devnull = open('/dev/null', 'w')
ms_blocker_marker_set_internal = False


# Exadata specific functions
def get_cell_usb_boot_dev():
  """ determine a cell's usb boot device """
  usb_boot_device = None
  if os.path.exists(imageinfo_cmd):
    usb_boot_device = subprocess.Popen([imageinfo_cmd, '-cbpart'], stderr=devnull, stdout=subprocess.PIPE).stdout.read().rstrip('\n')
  else:
    procfile = open("/proc/partitions")
    parts = [p.split() for p in procfile.readlines()[2:]]
    procfile.close()
    for part in parts:
      label = subprocess.Popen([e2label_cmd, '/dev/%s' % part[3]], stderr=devnull, stdout=subprocess.PIPE).stdout.read().rstrip('\n')
      if label == 'CELLBOOT':
        usb_boot_device = '/dev/' + part[3]
  return usb_boot_device

def is_cell():
  """ determine cell or compute node """
  if os.path.exists('/opt/oracle.cellos/ORACLE_CELL_NODE'):
    return True
  else:
    return False

def ms_actions(action):
  """ start/stop/restart/status the Managment Server """
  current_runlevel = int(subprocess.Popen([runlevel_cmd], stdout=subprocess.PIPE).stdout.read()[2])
  if check_service_config_state(service='celld', runlevel=current_runlevel, state='on'):
    cellrpm = True
  else:
    cellrpm = False
  if action == 'status':
    if cellrpm:
      proc = subprocess.Popen([cellcli_cmd, '-e', 'list cell attributes msstatus'], stderr=devnull, stdout=subprocess.PIPE)
      ms_status = proc.stdout.read().strip()
      return ms_status
  elif action == 'start':
    if cellrpm and subprocess.Popen([cellcli_cmd, '-e', 'list cell attributes msstatus'], stderr=devnull, stdout=subprocess.PIPE).stdout.read().strip() == 'stopped':
      subprocess.call([cellcli_cmd, '-e', 'alter cell startup services ms'], stdout=devnull, stderr=devnull)
  elif action == 'stop':
    if cellrpm and subprocess.Popen([cellcli_cmd, '-e', 'list cell attributes msstatus'], stderr=devnull, stdout=subprocess.PIPE).stdout.read().strip() == 'running':
      subprocess.call([cellcli_cmd, '-e', 'alter cell shutdown services ms'], stdout=devnull, stderr=devnull)
  elif action == 'restart':
    if cellrpm and subprocess.Popen([cellcli_cmd, '-e', 'list cell attributes msstatus'], stderr=devnull, stdout=subprocess.PIPE).stdout.read().strip() == 'running':
      subprocess.call([cellcli_cmd, '-e', 'alter cell restart services ms'], stdout=devnull, stderr=devnull)
    else:
      subprocess.call([cellcli_cmd, '-e', 'alter cell startup services ms'], stdout=devnull, stderr=devnull)

def ms_lock(mode=None):
  """ prevent MS from accessing the USB device """
  global ms_blocker_marker_set_internal
  ms_blocker_marker = '%s/system_in_transition' % os.getenv('EXADATA_IMG_TMP')

  if mode == 'lock':
    if not os.path.exists(ms_blocker_marker):
      open(ms_blocker_marker, 'a').close()
      ms_blocker_marker_set_internal = True
  elif mode == 'unlock':
    if os.path.exists(ms_blocker_marker) and ms_blocker_marker_set_internal:
      os.remove(ms_blocker_marker)
      ms_blocker_marker_set_internal = False
  else:
    raise Exception('In %s: Invalid mode passed as first argument. mode must be "lock" or "unlock". Got mode: %s' % ('ms_lock()', mode))


# configuration migration functions
def config_file_modify(srcfile, pattern, replace):
  """ file search and replace function """
  for line in fileinput.FileInput(srcfile, inplace=1):
    line = re.sub(pattern, replace, line)
    print line,
  fileinput.close()

def config_file_add_in(srcfile, pattern, insert, insert_after=True):
  """ file add line function """
  found = False
  f = open(srcfile, 'r')
  for line in f:
    if re.match(insert, line):
      found = True
  f.close
  if not found:
    for line in fileinput.FileInput(srcfile, inplace=1):
      if re.search(pattern, line):
        if insert_after:
          print line,
          print insert
        else:
          print insert
          print line,
      else:
        print line,
    fileinput.close()

def config_file_remove_from(srcfile, pattern):
  """ file remove line function """
  for line in fileinput.FileInput(srcfile, inplace=1):
    if not re.search(pattern, line):
      print line,
  fileinput.close()

# general purpose functions
def check_service_config_state(service, runlevel=3, state='on'):
  """ verify system services """
  service_status = subprocess.Popen([chkconfig_cmd, '--list', '%s' % service], stderr=devnull, stdout=subprocess.PIPE).stdout.read()
  if not service_status == '':
    if ':' + state in service_status.split()[runlevel+1]:
      return True
    else:
      return False
  else:
    return False


def check_service_state(service=None):
  """ verify system services """
  service_status = subprocess.Popen([service_cmd, service, 'status'], stderr=devnull, stdout=subprocess.PIPE).stdout.read().rstrip('\n')
  if not service_status == '':
    if 'running' in service_status:
      return True
    else:
      return False
  else:
    return False


def set_service_state(service=None, state=None, configure=None, activate=None):
  """ set system services on or off """
  if service is None or state is None or configure is None or activate is None:
    raise Exception('In %s: Invalid or incomplete arguments passed in. Arguments for function; service:%s, state:%s, configure:%s, activate:%s'
                    % ('set_service_state()', service, state, configure, activate))
  if state == 'enable' or state == 'on':
    chkconfig_state = 'on'
    service_state = 'start'
  elif state == 'disable' or state == 'off':
    chkconfig_state = 'off'
    service_state = 'stop'
  elif state == 'restart':
    service_state = 'restart'
  elif state is None:
    return False
  if configure:
    subprocess.Popen([chkconfig_cmd, service, chkconfig_state], stderr=devnull, stdout=subprocess.PIPE).stdout.read()
  if activate:
    subprocess.Popen([service_cmd, service, service_state], stderr=devnull, stdout=subprocess.PIPE).stdout.read()

def get_realuser():
  """ determine logged-in username """
  try:
    user =  os.getlogin()
  except:
    user =  pwd.getpwuid(os.getuid())[0]
  return user

def ilom_get(ilom_path='', ilom_param=''):
  """ Get ILOM settings """
  ilom_output = None
  ipmitool_cmd = ['ipmitool', 'sunoem', 'cli', 'force', 'show %s %s' % (ilom_path, ilom_param)]
  p = subprocess.Popen(ipmitool_cmd, shell=False, stdout=subprocess.PIPE, stderr=devnull)
  pout = p.communicate()[0]
  for line in pout.split('\n'):
    if '%s = ' % ilom_param in line:
      ilom_output = line.split()[2]
      break
  return ilom_output

def ilom_set(ilom_path='', ilom_param_value=''):
  """ Set ILOM settings """
  ilom_output = None
  ipmitool_cmd = ['ipmitool', 'sunoem', 'cli', 'force', 'set %s %s' % (ilom_path, ilom_param_value)]
  p = subprocess.Popen(ipmitool_cmd, shell=False, stdout=subprocess.PIPE, stderr=devnull)
  pout = p.communicate()[0]
  for line in pout.split('\n'):
    if line.lower().startswith("set '%s' to " % ilom_param_value.lower().split('=')[0]):
      ilom_output = ilom_param_value
      break
  return ilom_output

def is_binary(path):
  """ test for non-text file """
  return (re.search(r':.* text', subprocess.Popen([file_cmd, '-L', path], stdout=subprocess.PIPE).stdout.read()) is None)

def is_call_from(proc='000---000'):
  """ must supply a process name (not strict checking) """
  if 'Linux' in subprocess.Popen([uname_cmd, '-a'], stderr=devnull, stdout=subprocess.PIPE).stdout.read():
    pst = subprocess.Popen([pstree_cmd, '-Aacp', '%s' % os.getppid()], stderr=devnull, stdout=subprocess.PIPE).stdout.read()
  if proc in pst:
    return True
  else:
    return False

def is_ol6():
  """ determine if running on Oracle Linux 6 """
  pattern = re.compile('^kernel:.*el6.*')
  image_id_mnt = '/mnt/imaging/sys/opt/oracle.cellos/image.id'
  image_id = '/opt/oracle.cellos/image.id'
  if os.path.exists(image_id_mnt):
    file = image_id_mnt
  elif os.path.exists(image_id):
    file = image_id
  else:
    file = None
  try:
    f = open(file, 'r')
    for line in f:
      if pattern.match(line):
        return True
    return False
    f.close()
  except:
    return None

def make_salt(length):
  """ create a random salt for hashing """
  alphabet = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ/'
  chars = []
  for c in range(length):
    chars.append(random.choice(alphabet))
  return ''.join(chars)

def run_timed(command, timeout, proc_kill=False, output=False):
  """ run command within timeout and return its output,
      else (kill it and) return False"""
  cmd = command.split(" ")
  start = datetime.datetime.now()
  process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  while process.poll() is None:
    time.sleep(0.1)
    now = datetime.datetime.now()
    if (now - start).seconds > timeout:
      if proc_kill:
        os.kill(process.pid, signal.SIGKILL)
        os.waitpid(-1, os.WNOHANG)
      return False
  if output:
    return process.stdout.read()

def stdout_msg(level, message, exitcode, quiet=False):
  """ process user info and exit (if indicated) """
  if quiet:
    pass
  else:
    print '[%s][%s] %s' % (datestamp, level, message)
  if exitcode is not None:
    sys.exit(exitcode)

def who_is_calling(logfile):
   """ diagnostics: record who is calling """
   calling = subprocess.Popen([pstree_cmd, '-Aacp', '%s' % os.getppid()], stderr=devnull, stdout=subprocess.PIPE).stdout.read().rstrip('\n').split()[0]
   f = open(logfile, 'a')
   f.write('[' + datestamp + ']' + ' We are called by %s\n' % calling)
   f.write('-----------------------------\n')
   subprocess.call([pstree_cmd, '-Aacp', '%s' % os.getppid()], stderr=devnull, stdout=f)
   f.close()
