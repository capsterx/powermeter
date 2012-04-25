from collector import *
from newecm import *

CLASS = 'SocketServerCollector'

class SocketServerCollector(BufferedDataCollector):
    INSERT = True
    def __init__(self, packet_processor, *args, **kwargs):
        super(SocketServerCollector, self).__init__(packet_processor)
        socket.setdefaulttimeout(IP_TIMEOUT)
        print kwargs
        self._host = kwargs.get('ip_host') or IP_HOST
        self._port = int(kwargs.get('ip_port') or IP_PORT)
        self.sock = None
        self.conn = None
        infmsg('host: %s' % self._host)
        infmsg('port: %d' % self._port)

    def read_data(self):
      data = self.conn.recv(8096)
      if not data:
        raise Exception("socket closing")
      self.data_read(data)

    def read(self):
        try:
            print "Accepting socket...\n"
            self.conn, addr = self.sock.accept()
            super(SocketServerCollector, self).read();
        finally:
            print "Shutting down socket...."
            if self.conn:
              try:
                self.conn.shutdown(socket.SHUT_RD)
              except:
                pass
              try:
                self.conn.close()
              finally:
                self.conn = None

    def setup(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except: # REUSEPORT may not be supported on all systems  
            pass
        self.sock.bind((self._host, self._port))
        self.sock.listen(1)

    def cleanup(self):
        if self.sock:
            self.sock.close()
            self.sock = None
    
    @staticmethod
    def make_group(parser):
      pass
    
    @staticmethod
    def is_enabled(options):
      return options.ip_read and options.ip_mode == "server"
    
    @staticmethod
    def help():
      print '  --ip --ip-mode=server       read from TCP/IP as server'

    @staticmethod
    def make(options, procs):
      if options.ip_read and options.ip_mode == "server":
        return SocketServerCollector(procs, **vars(options))
      return None
