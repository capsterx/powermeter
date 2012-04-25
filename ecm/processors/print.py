from newecm import *
from processor import *
from ecm_decoder import *
from utils import *
from collector import *
from config import *

SUPPORT = 'Print'
CLASS = 'PrintProcessor'


class PrintProcessor(BaseProcessor):
    INSERT = 'True'
    def __init__(self, *args, **kwargs):
        super(PrintProcessor, self).__init__(*args, **kwargs)
        self.prev_packet = {}
        self.map_str     = kwargs.get('print_map')   or ''

    def process_compiled(self, packet, packet_buffer):
        sn = getserial(packet)
        if sn in self.prev_packet:
            try:
                p = calculate(packet, self.prev_packet[sn])
            except ZeroDivisionError, zde:
                infmsg("not enough data in buffer for %s" % sn)
                return
            except CounterResetError, cre:
                wrnmsg("counter reset for %s: %s" % (sn, cre.msg))
                return
            self.process_calculated(sn, [p])
        self.prev_packet[sn] = packet

    def setup(self):
        self.map = pairs2dict(self.map_str)
    
    def get(self, ecm_serial, s, name):
      key = ecm_serial + '_' + s.lower()
      return self.map.get(key, s) + " " + name + ":"


    def process_calculated(self, ecm_serial, packets):
        for p in packets:
            ts = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(p['time_created']))

            # start with newline in case previous run was stopped in the middle
            # of a line.  this ensures that the new output is not attached to
            # some old incompletely written line.
            print
            print ts+": ECM: %s" % ecm_serial
            print ts+": Counter: %d" % getresetcounter(p['flag'])
            print ts+": Volts:              %9.2fV" % (p['volts'])
            print ts+": %-16.16s  %9.2fA" % (self.get(ecm_serial, 'CH1', 'Amps'), p['ch1_amps'])
            print ts+": %-16.16s  %9.2fA" % (self.get(ecm_serial, 'CH2', 'Amps'), p['ch2_amps'])
            print ts+": %-17.17s  % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'CH1', 'Watts'), p['ch1_wh'] , p['ch1_watts'])
            print ts+": %-17.17s  % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'CH1', '+Watts'), p['ch1_pwh'], p['ch1_positive_watts'])
            print ts+": %-17.17s  % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'CH1', '-Watts'), p['ch1_nwh'], p['ch1_negative_watts'])
            print ts+": %-17.17s  % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'CH2', 'Watts'), p['ch2_wh'] , p['ch2_watts'])
            print ts+": %-17.17s  % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'CH2', '+Watts'), p['ch2_pwh'], p['ch2_positive_watts'])
            print ts+": %-17.17s  % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'CH2', '-Watts'), p['ch2_nwh'], p['ch2_negative_watts'])
            print ts+": %-18.18s % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'AUX1', 'Watts'), p['aux1_wh'], p['aux1_watts'])
            print ts+": %-18.18s % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'AUX2', 'Watts'), p['aux2_wh'], p['aux2_watts'])
            print ts+": %-18.18s % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'AUX3', 'Watts'), p['aux3_wh'], p['aux3_watts'])
            print ts+": %-18.18s % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'AUX4', 'Watts'), p['aux4_wh'], p['aux4_watts'])
            print ts+": %-18.18s % 13.6fKWh (% 5dW)" % (self.get(ecm_serial, 'AUX5', 'Watts'), p['aux5_wh'], p['aux5_watts'])

    @staticmethod
    def make_group(parser):
      pass

    @staticmethod
    def is_enabled(options):
      return options.print_out

    @staticmethod
    def help():
      print '  --print              print to screen'

    @staticmethod
    def make(options, procs):
      if options.print_out:
          procs.append(PrintProcessor(**vars(options)))
