'''
OpenEnergyMonitor Configuration

1) register for an account
2) obtain the API key

Register for an account at the emoncms web site.

Obtain the API key with write access.

By default, all channels on all ECMs will be uploaded.

For example, this configuration will upload all data from all ECMs.

[openenergymonitor]
oem_out=true
oem_token=xxx
'''

from newecm import *
from upload import *
from collector import *

SUPPORT='OpenEnergyMonitor'
CLASS = 'OpenEnergyMonitorProcessor'

class Constants:
# open energy monitor emoncms defaults
  OEM_URL           = 'https://localhost/emoncms3/api/post'
  OEM_UPLOAD_PERIOD = MINUTE
  OEM_TIMEOUT       = 15 # secons
  OEM_TOKEN         = ''

class OpenEnergyMonitorProcessor(UploadProcessor):
  INSERT = True
  def __init__(self, *args, **kwargs):
    super(OpenEnergyMonitorProcessor, self).__init__(*args, **kwargs)
    self.url     = kwargs.get('oem_url')   or OEM_URL
    self.token   = kwargs.get('oem_token') or OEM_TOKEN
    self.upload_period = OEM_UPLOAD_PERIOD
    self.timeout = OEM_TIMEOUT

    infmsg('OEM: upload period: %d' % self.upload_period)
    infmsg('OEM: url: ' + self.url)
    infmsg('OEM: token: ' + self.token)

  def setup(self):
    if not (self.url and self.token):
      print 'OpenEnergyMonitor Error: Insufficient parameters'
      if not self.url:
          print '  A URL is required'
      if not self.url:
          print '  A token is required'
      sys.exit(1)

  def process_calculated(self, ecm_serial, packets):
    for p in packets:
      data = []
      for idx,c in enumerate(ECM1240_CHANNELS):
        dpkey = obfuscate_serial(ecm_serial) + '_' + c
        data.append('%s_w:%d' % (dpkey, p[c+'_watts']))
      if len(data):
        url = '%s?apikey=%s&time=%s&json={%s}' % (
            self.url, self.token, p['time_created'], ','.join(data))
        result = self._urlopen(ecm_serial, url, '')
    self.mark_upload_complete(ecm_serial)

  def _create_request(self, url):
    req = super(OpenEnergyMonitorProcessor, self)._create_request(url)
    return req

  def _handle_urlopen_error(self, e, sn, url, payload):
    errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                    '\n  ECM:   ' + sn,
                    '\n  URL:   ' + url,
                    '\n  token: ' + self.token,
                    '\n  data:  ' + payload,]))
  @staticmethod
  def make_group(parser):
    group = optparse.OptionGroup(parser, 'OpenEnergyMonitor options')
    group.add_option('--oem', action='store_true', dest='oem_out', default=False, help='upload data using OpenEnergyMonitor API')
    group.add_option('--oem-url', help='URL')
    group.add_option('--oem-token', help='token')
    parser.add_option_group(group)

  @staticmethod
  def is_enabled(options):
    return options.oem_out

  @staticmethod
  def help():
    print '  --oem                upload to OpenEnergyMonitor'

  @staticmethod
  def make(options, procs):
    if options.oem_out:
      procs.append(OpenEnergyMonitorProcessor(**vars(options)))
