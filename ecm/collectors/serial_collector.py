from newecm import *
from collector import *

SUPPORT = 'Serial'
CLASS = 'SerialCollector'

class Constants:
# serial settings
# the com/serial port the ecm is connected to (COM4, /dev/ttyS01, etc)
  SERIAL_PORT = "/dev/ttyUSB0"
  SERIAL_BAUD = 19200       # the baud rate we talk to the ecm


class SerialCollector(BufferedDataCollector):
    INSERT = True
    def __init__(self, packet_processor, *args, **kwargs):
        super(SerialCollector, self).__init__(packet_processor)

        if not serial:
            print 'Error: serial module could not be imported.'
            sys.exit(1)

        self._port  = kwargs.get('serial_port') or SERIAL_PORT
        self._baudrate = int(kwargs.get('serial_baud') or SERIAL_BAUD)
        self.conn = None

        infmsg('serial port: %s' % self._port)
        infmsg('baud rate: %d' % self._baudrate)

    def read_data(self):
        self.data_read(self.conn.read(8096))

    def setup(self):
        self.conn = serial.Serial(self._port, self._baudrate, timeout=0)
        self.conn.open()

    def cleanup(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'serail read options')
      group.add_option('--serial', action='store_true', dest='serial_read', default=False, help='read from serial port')
      group.add_option('--serialport', dest='serial_port', help='serial port')
      group.add_option('--baudrate', dest='serial_baud', help='serial baud rate')
      parser.add_option_group(group)
    
    @staticmethod
    def is_enabled(options):
      return options.serial_read

    @staticmethod
    def help():
      print '  --serial     read from serial'

    def make(options, procs):
      if options.serial_read:
          return SerialCollector(procs, **vars(options))
      return None
