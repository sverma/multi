import paramiko
import argparse
from threading import Thread
from threading import Lock,RLock
from Queue import Queue
import io
import hashlib
import socket
from urllib2 import Request, urlopen, URLError, HTTPError

# Signal Handling
# Thread Reaping

class Exec:

  lock = RLock()
  t_lock = RLock()

  def __init__(self,args):
    self.args = args
    self.output = {}

  def run(self):
    task = self.args.task
    num_threads = int(self.args.p)
    t_p = ThreadPool(num_threads)
    self.timeout =  float(self.args.t)
    if task == "exec":
      self.servers = self.calculate_servers()
      self.command = self.args.command
      self.combine = self.args.combine

      for server in self.servers:
        t_p.add_task(self.run_command,server,self.command,self.timeout)

      t_p.wait_completion()
      if(self.combine):
        self.show_combined_output()

  def calculate_servers(self):
    if (not self.args.servers == None ) and (not self.args.inventory == None):
      print("You have specified both list of servers and inventory File, Please choose only one option")
      exit(0)
    else:
      if  self.args.inventory:
        servers = self.read_inventory_file(self.args.inventory)
      else:
        servers = self.args.servers.split(',')
      return servers

  def read_inventory_file(self, inventory):
    try:
      with io.open(inventory) as file:
        lines = file.readlines()
        servers = []
        for line in lines:
          servers.extend(line.rstrip().split(','))
        return [str(x) for x in servers]
    except Exception as e:
      print("Exception occured while reading the File {0} : {1}".format(inventory,e))


  def ssh_connection(self,server,timeout):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server,timeout=float(timeout))
    return(ssh)

  def execute_command(self,command, conn, server):
    stdin,stdout,stderr = conn.exec_command(command)
    outlines = stdout.readlines()
    resp=''.join(outlines)
    self.store_output(server,resp)
    return(resp)

  def store_output(self,server,resp):
    m = hashlib.md5()
    m.update(resp)
    k = m.hexdigest()
    if k not in self.output.keys():
      Exec.lock.acquire()
      self.output[k] = ([server], resp)
      Exec.lock.release()
    else:
      Exec.lock.acquire()
      self.output[k][0].append(server)
      Exec.lock.release()

  def run_command(self,server,command,timeout):
    try:
      conn = self.ssh_connection(server,timeout)
    except Exception as e:
      Exec.t_lock.acquire()
      print "#### {0} ####".format(server)
      print("Exception in ssh connect to server {1}: {0}".format(e,server))
      Exec.t_lock.release()
    else:
      output= self.execute_command(command,conn,server)

      Exec.t_lock.acquire()
      print "#### {0} ####".format(server)
      print(output)
      Exec.t_lock.release()

  def show_combined_output(self):
    for k,i in self.output.items():
      servers_l = i[0]
      output = i[1]
      if len(servers_l) > 1:
        servers = ','.join(servers_l)
        print("{0} :".format(servers))
        print(output)


class Curl:
  lock = RLock()
  t_lock = RLock()

  def __init__(self, args):
    self.args = args
    self.output = {}

  def run(self):
    self.hostnames = self.args.urls.split(',')
    t_p = ThreadPool(30)
    print(self.args)
    self.timeout = float(self.args.timeout)
    for hostname in self.hostnames:
      t_p.add_task(self.run_test, hostname, self.timeout)

    t_p.wait_completion()

  def run_test(self, hostname, timeout):
    socket.setdefaulttimeout(timeout)
    self.output[hostname] = []

    try:
      req = Request('http://' + hostname)
      r = urlopen(req)
      if r:
        self.output[hostname].append(CurlResult(hostname,"PASS",False,"","",'80'))
    except HTTPError as e:
      self.output[hostname].append(CurlResult(hostname,"PASS", True, str(e), "",'80'))
    except Exception as e:
      self.output[hostname].append(CurlResult(hostname,"FAIL", False, "", str(e),'80'))

    try:
      req = Request('https://' + hostname)
      r = urlopen(req)
      if r:
        self.output[hostname].append(CurlResult(hostname,"PASS", False, "", "", '443'))
    except HTTPError as e:
      self.output[hostname].append(CurlResult(hostname,"PASS", True, str(e), "", '443'))
    except Exception as e:
      self.output[hostname].append(CurlResult(hostname,"FAIL", False, "", str(e), '443'))



  def print_output(self):

    Curl.t_lock.acquire()
    for hostname in self.output.keys():
      for port in self.output[hostname]:
        print(port.format_line())
    Curl.t_lock.release()


class CurlResult:

  def __init__(self,hostname,result,error,error_string,exception,port):
    self.hostname = hostname
    self.result = result
    self.error = error
    self.error_string = error_string
    self.execption = exception
    self.port = port

  def format_line(self):
    port_str = "Port {0}:".format(self.port)
    conn_str = "Connection {0}".format(self.result)
    if self.error:
      err_string = "HTTTP Error {0}".format(self.error_string)
    elif (not self.error) and self.result == "FAIL":
      err_string = "Exception: {0}".format(self.execption)
    else:
      err_string = "NO HTTP ERROR"

    return '{0:30} {1:10} {2:20}, {3}'.format(self.hostname, port_str, conn_str, err_string)


class Worker(Thread):
  """Thread executing tasks from a given tasks queue"""
  def __init__(self,tasks):
    Thread.__init__(self)
    self.tasks = tasks
    self.daemon = True
    self.start()

  def run(self):
    while True:
      func, args, kargs = self.tasks.get()
      try:
        func(*args,**kargs)
      except Exception, e:
        print(e)
      self.tasks.task_done()

class ThreadPool:
  """Pool of threads consuming tasks from the queue"""
  def __init__(self,num_threads):
    self.tasks = Queue(num_threads)
    for _ in range(num_threads): Worker(self.tasks)

  def add_task(self,func, *args, **kargs):
    self.tasks.put((func,args,kargs))

  def wait_completion(self):
    """Wait for completion of all the tasks in the queue"""
    self.tasks.join()

def parse_args():
    parser = argparse.ArgumentParser(add_help="Do operations on multiple machines in parallel")
    subparsers = parser.add_subparsers(dest='task')
    parser_exec = subparsers.add_parser('exec')
    parser_upload = subparsers.add_parser('upload')
    parser_curl= subparsers.add_parser('curl')
    parser_exec.add_argument('--servers','-s', help="Comma seperated list of servers")
    parser_exec.add_argument('-c', '--command', help="Command to execute on servers", required=True)
    parser_exec.add_argument('--combine', help="Combine the output of multiple servers", required=False, action='store_true')
    parser_exec.add_argument('--inventory', help="The Server Inventory file")
    parser.add_argument('-t', help="SSH connect timeout", default=10)
    parser.add_argument('-p', help="Parallelism , How many parallel threads to start, Default 10", default=10)
    parser_upload.add_argument('--servers','-s', help="Comma seperated list of servers", required=True)
    parser_upload.add_argument('--remote_path','-r', help="Path on the remote server", required=True)
    parser_upload.add_argument('--user','-u', help="SSH User to connect to", required=False)
    parser_exec.add_argument('--user','-u', help="SSH User to connect to", required=False)
    parser_upload.add_argument('-i', help="SSH private key file path", required=False)
    parser_exec.add_argument('-i', help="SSH private key file path", required=False)
    parser_curl.add_argument('--urls','-u', help="Comma seperated list of hostnames")
    parser_curl.add_argument('-t', '--timeout', help="Curl Download Timeout")
    args = parser.parse_args()
    return(args)


if __name__ == "__main__":
  args = parse_args()
  task = args.task
  if ( task == "exec"):
    mexec = Exec(args)
    mexec.run()
  if ( task == 'curl'):
    curl = Curl(args)
    curl.run()
    curl.print_output()
