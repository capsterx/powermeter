from newecm import *
from processor import *

SUPPORT = 'Raw Server'
CLASS = 'RawServer'

class RawServer(BaseProcessor):
  INSERT = True
  def __init__(self, *args, **kwargs):
    super(RawServer, self).__init__(*args, **kwargs)
    self.host        = kwargs.get('host')
    self.port     = int(kwargs.get('port'))
    self.hosts = []
    self.server = None

  def setup(self):
    eventlet.spawn_n(self.run)

  def run(self):
    self.server = eventlet.listen((self.host, self.port))
    while True:
        try:
            print "Trying accept"
            new_sock, address = self.server.accept()
            print "accepted", address
            self.hosts.append(new_sock)
            print self.hosts
            self.test_all()
        except (SystemExit, KeyboardInterrupt):
            break
        except Exception, e:
          traceback.print_exc()
          self.test_all()
          pass

  def write_all(self, s):
    self.hosts[:] = [host for host in self.hosts if self.do_write(host, s)]
  
  def test_all(self):
    print "Selecting on %s" % self.hosts
    r, w, e = select.select(self.hosts, [], [], 0)
    print "Checking up on %s" % r
    for h in r:
      if not h.recv(1024):
        print "Removing host %s" % h
        self.hosts.remove(h)

  def try_read(self, host):
    try:
      f = host.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
      if not f:
        return False
      print "Host is ok '%s'" % f
      return True
    except Exception, e:
      print "Host is not ok"
      host.close()
      traceback.print_exc()
      return False
  
  def cleanup(self):
    for host in self.hosts:
      print "Closing host for cleanup"
      host.close()
    if self.server:
      print "Closing server"
      self.server.close()
      self.server = None
    self.hosts = None

  def do_write(self, host, full_packet):
    try:
      host.send(full_packet)
      #host.flush()
      print "Wrote packet"
      return True
    except:
      print "Error"
      return False
   
  def process_raw_compiled(self, full_packet, packet, packet_buffer):
    self.write_all(full_packet)
    print "Hosts left %s" % self.hosts

  @staticmethod
  def make_group(parser):
    group = optparse.OptionGroup(parser, 'Raw server emulation')
    group.add_option('--raw_server', action='store_true', dest='raw_server_out')
    group.add_option('--raw_server_host', action='store')
    group.add_option('--raw_server_port', action='store', type=int)
    parser.add_option_group(group)

  @staticmethod
  def is_enabled(options):
    return options.raw_server_out

  @staticmethod
  def help():
    print '  --raw_server            emulate a raw tcp server'

  @staticmethod
  def make(options, procs):
    if options.raw_server_out:
      if not eventlet:
        print "Can't emulate raw server without eventlet"
        sys.exit(1)
      else:
        procs.append(RawServer(**{
                    'port':       options.raw_server_port,
                    'host':       options.raw_server_host}))
