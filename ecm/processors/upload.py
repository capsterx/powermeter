from processor import *
from ecm_decoder import *
from utils import *
from newecm import *
from collector import *
#### END HEADER

class UploadProcessor(BaseProcessor):
    class FakeResult(object):
        def geturl(self):
            return 'fake result url'
        def info(self):
            return 'fake result info'
        def read(self):
            return 'fake result read'

    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_UPLOAD_TIMEOUT
        self.upload_period = DEFAULT_UPLOAD_PERIOD
        self.last_upload = {}
        self.urlopener = {}
        pass

    def setup(self):
        pass

    def process_calculated(self, ecm_serial, packets):
        pass
    
    def process_compiled(self, packet, packet_buffer):
        sn = getserial(packet)
        if not self.time_to_upload(sn):
            return
        packets = []
        data = packet_buffer.data_over(sn, self.upload_period)
        dbgmsg('%d packets to process' % len(data))
        for a,b in zip(data[0:], data[1:]):
            packets.append(calculate(b[1],a[1]))
        self.process_calculated(sn, packets)
  
    def handle(self, exception):
        return False

    def cleanup(self):
        pass

    def time_to_upload(self, sn):
        now = getgmtime()
        if sn in self.last_upload and now < (self.last_upload[sn] + self.upload_period):
            return False
        return True

    def mark_upload_complete(self, sn):
        self.last_upload[sn] = getgmtime()

    def _create_request(self, url):
        req = urllib2.Request(url)
        req.add_header("User-Agent", "ecmread")
        return req

    def _urlopen(self, sn, url, data):
        try:
            req = self._create_request(url)
            dbgmsg('%s: url: %s\n  headers: %s\n  data: %s' %
                   (self.__class__.__name__, req.get_full_url(), req.headers, data))

            result = {}
            if SKIP_UPLOAD:
                result = UploadProcessor.FakeResult()
            elif self.urlopener:
                result = self.urlopener.open(req, data, self.timeout)
            else:
                result = urllib2.urlopen(req, data, self.timeout)

            infmsg('%s: %d bytes uploaded for %s' %
                   (self.__class__.__name__, len(data), sn))
            dbgmsg('%s: url: %s\n  response: %s' %
                   (self.__class__.__name__, result.geturl(), result.info()))
            return result
        except urllib2.HTTPError, e:
            self._handle_urlopen_error(e, sn, url, data)
            result = e.read()
            errmsg(e)
            errmsg(result)

    def _handle_urlopen_error(self, e, sn, url, data):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  ECM:  ' + sn,
                        '\n  URL:  ' + url,
                        '\n  data: ' + data,]))
