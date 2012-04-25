from newecm import *
from processor import *

SUPPORT = 'Raw Client'
CLASS = 'RawClient'

class RawClient(BaseProcessor):
  INSERT = True
  def __init__(self, *args, **kwargs):
    super(RawClient, self).__init__(*args, **kwargs)
    self.host        = kwargs.get('host')
    self.port     = int(kwargs.get('port'))
    self.socket = None

  def setup(self):
    eventlet.spawn_n(self.run)

  def run(self):
    while not self.socket:
      try:
        self.socket = eventlet.connect((self.host, self.port))
      except:
        time.sleep(5)
       
    print "Connected to client"
  
  def cleanup(self):
    if self.socket:
      self.socket.close()
      self.socket = None

  def process_raw_compiled(self, full_packet, packet, packet_buffer):
    try:
      if self.socket:
        print "Writing to host... %s" % full_packet
        self.socket.send(full_packet)
        print "Wrote packet"
      return True
    except Exception, e:
      if self.socket:
        self.socket.close()
        self.socket = None
      print "Error"
      wrnmsg('Exception in %s: %s' % (self.__class__.__name__, e))
      if LOGLEVEL >= LOG_DEBUG:
          traceback.print_exc()
      eventlet.spawn_n(self.run)

  @staticmethod
  def make_group(parser):
    group = optparse.OptionGroup(parser, 'Raw client emulation')
    group.add_option('--raw_client', action='store_true', dest='raw_client_out')
    group.add_option('--raw_client_host', action='store')
    group.add_option('--raw_client_port', action='store', type=int)
    parser.add_option_group(group)

  @staticmethod
  def is_enabled(options):
    return options.raw_client_out

  @staticmethod
  def help():
    print '  --raw_client            serve the raw data to a host'

  @staticmethod
  def make(options, procs):
    if options.raw_client_out:
      if not eventlet:
        print "Can't emulate raw client without eventlet"
        sys.exit(1)
      else:
        procs.append(RawClient(**{
                    'port':       options.raw_client_port,
                    'host':       options.raw_client_host}))
