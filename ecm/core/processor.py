from registry import *
#### END HEADER

class ProcessorRegistry(Registry):
  registered = {}
  pass

class BaseProcessor(object):
    __metaclass__ = ProcessorRegistry
    def __init__(self, *args, **kwargs):
        pass

    def setup(self):
        pass

    def process_calculated(self, ecm_serial, packets):
        pass

    def process_compiled(self, packet, packet_buffer):
      pass

    def process_raw_compiled(self, full_packet, packet, packet_buffer):
        self.process_compiled(packet, packet_buffer)
  
    def handle(self, exception):
        return False

    def cleanup(self):
        pass
