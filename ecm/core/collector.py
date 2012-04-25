from registry import *
from utils import *
from buffer_utils import *
from ecm_decoder import *
#### END HEADER

# settings for ECM-1240 packets
START_HEADER0    = 254
START_HEADER1    = 255
ECM1240_PACKET_ID = 3
END_HEADER0       = 255
END_HEADER1       = 254
DATA_BYTES_LENGTH = 59             # does not include the start/end headers
SEC_COUNTER_MAX   = 16777216
ECM1240_UNIT_ID   = 3
ECM1240_CHANNELS  = ['ch1', 'ch2', 'aux1', 'aux2', 'aux3', 'aux4', 'aux5']

def calculate_checksum(packet):
    '''calculate the packet checksum'''
    checksum = START_HEADER0
    checksum += START_HEADER1
    checksum += ECM1240_PACKET_ID
    checksum += sum([ord(c) for c in packet])
    checksum += END_HEADER0
    checksum += END_HEADER1
    return checksum & 0xff

def calculate(now, prev):
    '''calc average watts/s between packets'''

    # if reset counter has changed since last packet, skip the calculation
    c0 = getresetcounter(prev['flag'])
    c1 = getresetcounter(now['flag'])
    if c1 != c0:
        raise CounterResetError("old: %d new: %d" % (c0, c1))

    if now['secs'] < prev['secs']:
        now['secs'] += SEC_COUNTER_MAX # handle seconds counter overflow
    secs_delta = float(now['secs'] - prev['secs'])

    ret = now

    # CH1/2 Watts
    ret['ch1_watts'] = (ret['ch1_aws'] - prev['ch1_aws']) / secs_delta
    ret['ch2_watts'] = (ret['ch2_aws'] - prev['ch2_aws']) / secs_delta

    ret['ch1_positive_watts'] = (ret['ch1_pws'] - prev['ch1_pws']) / secs_delta
    ret['ch2_positive_watts'] = (ret['ch2_pws'] - prev['ch2_pws']) / secs_delta

    ret['ch1_negative_watts'] = ret['ch1_watts'] - ret['ch1_positive_watts']
    ret['ch2_negative_watts'] = ret['ch2_watts'] - ret['ch2_positive_watts']

    # All Watts increase no matter which way the current is going
    # Polar Watts only increase if the current is positive
    # Every Polar Watt does register as an All Watt too.
    # math comes to: Watts = 2x Polar Watts - All Watts
    ret['ch1_pwh'] = ret['ch1_pws'] / 3600000.0
    ret['ch2_pwh'] = ret['ch2_pws'] / 3600000.0
    ret['ch1_nwh'] = (ret['ch1_aws'] - ret['ch1_pws']) / 3600000.0
    ret['ch2_nwh'] = (ret['ch2_aws'] - ret['ch2_pws']) / 3600000.0
    ret['ch1_wh']  = ret['ch1_pwh'] - ret['ch1_nwh']
    ret['ch2_wh']  = ret['ch2_pwh'] - ret['ch2_nwh']

    ret['aux1_wh'] = ret['aux1_ws'] / 3600000.0
    ret['aux2_wh'] = ret['aux2_ws'] / 3600000.0
    ret['aux3_wh'] = ret['aux3_ws'] / 3600000.0
    ret['aux4_wh'] = ret['aux4_ws'] / 3600000.0
    ret['aux5_wh'] = ret['aux5_ws'] / 3600000.0

    # Polar Watts' instant value's only purpose seems to help find out if
    # main watts are positive or negative. Polar Watts only goes up if the
    # sign is positive. If they are null, tha means the sign is negative.
    if (ret['ch1_positive_watts'] == 0):
        ret['ch1_watts'] *= -1 
    if (ret['ch2_positive_watts'] == 0):
        ret['ch2_watts'] *= -1 

    # AUX1-5 Watts
    ret['aux1_watts'] = (ret['aux1_ws'] - prev['aux1_ws']) / secs_delta
    ret['aux2_watts'] = (ret['aux2_ws'] - prev['aux2_ws']) / secs_delta
    ret['aux3_watts'] = (ret['aux3_ws'] - prev['aux3_ws']) / secs_delta
    ret['aux4_watts'] = (ret['aux4_ws'] - prev['aux4_ws']) / secs_delta
    ret['aux5_watts'] = (ret['aux5_ws'] - prev['aux5_ws']) / secs_delta

    return ret

class Buffer:
  def __init__(self):
    self.buf = ""

  def __len__(self):
    return len(self.buf)

  def get_bytes(self, start, end):
    return self.buf[start:start+end]

  def get_byte(self, num):
    if len(self.buf) < num + 1:
        raise Exception("Unepected request %d - %d" % (num, len(self.buf)))
    byte = ord(self.buf[num])
    return byte

  def remove(self, num):
    ret = self.buf[0:num]
    self.buf = self.buf[num:]
    return ret

  def append(self, buf):
    self.buf += buf

class CollectorRegistry(Registry):
  registered = {}
  pass

class BaseDataCollector(object):
    __metaclass__ = CollectorRegistry
    def __init__(self, packet_processor):
        self.packet_processor = packet_processor
        dbgmsg('using %d processors:' % len(self.packet_processor))
        for p in self.packet_processor:
            dbgmsg('  %s' % p.__class__.__name__)

    def setup(self):
        pass

    def cleanup(self):
        pass

    # The read method collects data then passes it to each of the processors.
    def read(self):
        pass

    # Loop forever, break only for keyboard interrupts.
    def run(self):
        try:
            self.setup()
            for p in self.packet_processor:
                dbgmsg('setup %s' % p.__class__.__name__)
                p.setup()
                dbgmsg('setup done')

            while True:
                try:
                    self.read()
                except KeyboardInterrupt, e:
                    raise e
                except Exception, e:
                    traceback.print_exc()
                    wrnmsg(e)

        except KeyboardInterrupt:
            sys.exit(0)
        except Exception, e:
            if LOGLEVEL >= LOG_DEBUG:
                traceback.print_exc()
            else:
                errmsg(e)
            sys.exit(1)

        finally:
            for p in self.packet_processor:
                dbgmsg('cleanup %s' % p.__class__.__name__)
                p.cleanup()
            self.cleanup()


class BufferedDataCollector(BaseDataCollector):
    def __init__(self, packet_processor):
        super(BufferedDataCollector, self).__init__(packet_processor)
        self.packet_buffer = CompoundBuffer(BUFFER_TIMEFRAME)
        dbgmsg('buffer size is %d' % BUFFER_TIMEFRAME)
        self.buf = Buffer()

    def _convert(self, bytes):
        return reduce(lambda x,y:x+y[0] * (256**y[1]), zip(bytes,xrange(len(bytes))),0)

    def _compile(self, packet):
        now = {}

        # Voltage Data (2 bytes)
        now['volts'] = 0.1 * self._convert(packet[1::-1])

        # CH1-2 Absolute Watt-Second Counter (5 bytes each)
        now['ch1_aws'] = self._convert(packet[2:7])
        now['ch2_aws'] = self._convert(packet[7:12])

        # CH1-2 Polarized Watt-Second Counter (5 bytes each)
        now['ch1_pws'] = self._convert(packet[12:17])
        now['ch2_pws'] = self._convert(packet[17:22])

        # Reserved (4 bytes)

        # Device Serial Number (2 bytes)
        now['ser_no'] = self._convert(packet[26:28])

        # Reset and Polarity Information (1 byte)
        now['flag'] = self._convert(packet[28:29])

        # Device Information (1 byte)
        now['unit_id'] = self._convert(packet[29:30])

        # CH1-2 Current (2 bytes each)
        now['ch1_amps'] = 0.01 * self._convert(packet[30:32])
        now['ch2_amps'] = 0.01 * self._convert(packet[32:34])

        # Seconds (3 bytes)
        now['secs'] = self._convert(packet[34:37])

        # AUX1-5 Watt-Second Counter (4 bytes each)
        now['aux1_ws'] = self._convert(packet[37:41])
        now['aux2_ws'] = self._convert(packet[41:45])
        now['aux3_ws'] = self._convert(packet[45:49])
        now['aux4_ws'] = self._convert(packet[49:53])
        now['aux5_ws'] = self._convert(packet[53:57])

        # DC voltage on AUX5 (2 bytes)
        now['aux5_volts'] = self._convert(packet[57:59])

        return now

    def read(self):
      while True:
        self.read_data()

    # Called by derived classes when data has been read
    def data_read(self, new_data):
      self.buf.append(new_data)
      data = self.buf
      while len(data) >= 65:
        header = [START_HEADER0, START_HEADER1, ECM1240_PACKET_ID]
        valid = True
        for i in range(len(header)):
          byte = data.get_byte(i)
          if byte != header[i]:
              dbgmsg("expected START_HEADER%d %s, got %s" %
                     (i, hex(header[i]), hex(byte)))
              data.remove(i + 1)
              valid = False
              break
        if not valid:
          continue

        packet = data.get_bytes(len(header), DATA_BYTES_LENGTH)

        footer = [END_HEADER0, END_HEADER1]
        for i in range(len(footer)):
          byte = data.get_byte(len(header) + DATA_BYTES_LENGTH + i)
          if byte != footer[i]:
              dbgmsg("expected END_HEADER%d %s, got %s" %
                     (i, hex(footer[i]), hex(byte)))
              data.remove(len(header) + DATA_BYTES_LENGTH + i + 1)
              valid = False
              break
        if not valid:
          continue

        # we only handle ecm-1240 devices
        uid = ord(packet[29:30])
        if uid != ECM1240_UNIT_ID:
            infmsg("unrecognized unit id: expected %s, got %s" %
                   (hex(ECM1240_UNIT_ID), hex(uid)))
            data.remove(59 + 3 + 2)
            continue

        # if the checksum is incorrect, ignore the packet
        checksum = calculate_checksum(packet)
        print "Checking byte %d" % (len(header) + DATA_BYTES_LENGTH + len(footer))
        byte = data.get_byte(64)
        full_packet = data.remove(65)
        if byte != checksum:
            infmsg("bad checksum for %s: expected %s, got %s" %
                   (getserialraw(packet), hex(checksum), hex(byte)))
            continue

        packet = [ord(c) for c in packet]
        packet = self._compile(packet)
        print "Processing packet"
        packet['time_created'] = getgmtime()
        self.packet_buffer.insert(packet['time_created'], packet)
        self.process(full_packet, packet)

    # process a compiled packet
    def process(self, full_data, packet):
        for p in self.packet_processor:
            try:
                dbgmsg('processing with %s' % p.__class__.__name__)
                p.process_raw_compiled(full_data, packet, self.packet_buffer)
            except Exception, e:
                if not p.handle(e):
                    wrnmsg('Exception in %s: %s' % (p.__class__.__name__, e))
                    if LOGLEVEL >= LOG_DEBUG:
                        traceback.print_exc()
