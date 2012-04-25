from collector import *
from newecm import *

CLASS = 'SocketClientCollector'

class SocketClientCollector(BufferedDataCollector):
    INSERT = True
    def __init__(self, packet_processor, **kwargs):
        super(SocketClientCollector, self).__init__(packet_processor)
        socket.setdefaulttimeout(IP_TIMEOUT)
        self._host = kwargs.get('ip_host') or IP_HOST
        self._port = int(kwargs.get('ip_port') or IP_PORT)
        self.sock = None
        infmsg('host: %s' % self._host)
        infmsg('port: %d' % self._port)

    def read_data(self):
      data = self.sock.recv(8096)
      if not data:
        raise Exception("socket closing")
      self.data_read(data)

    def read(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self._host, self._port))
            super(SocketClientCollector, self).read();
        finally:
            if self.sock:
                print "Closing socket"
                try:
                  self.sock.close()
                finally:
                  self.sock = None
    
    @staticmethod
    def make_group(parser):
      pass
    
    @staticmethod
    def is_enabled(options):
      return options.ip_read and options.ip_mode == "client"
    
    @staticmethod
    def help():
      print '  --ip --ip-mode=client       read from TCP/IP as clinet'

    @staticmethod
    def make(options, procs):
      if options.ip_read and options.ip_mode == "client":
        return SocketClientCollector(procs, **vars(options))
      return None
