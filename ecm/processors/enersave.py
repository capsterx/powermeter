'''
EnerSave Configuration:

1) create an account
2) obtain a token
3) optionally indicate which channels to record and assign labels/types

Create an account at the enersave web site.  Specify 'other' as the device
type, then enter ECM-1240.

To obtain the token, enter this URL in a web browser:

https://data.myenersave.com/fetcher/bind?mfg=Brultech&model=ECM-1240

Define labels for ECM channels as a comma-delimited list of tuples.  Each
tuple contains an id, description, type.  If no map is specified, data from
all channels will be uploaded, generic labels will be assigned, and the type
for each channel will default to net metering.

EnerSave defines the following types:

   0 - load
   1 - generation
   2 - net metering (default)
  10 - standalone load
  11 - standalone generation
  12 - standalone net

For example, via configuration file:

[enersave]
es_token=XXX
es_map=399999_ch1,kitchen,,399999_ch2,solar array,1,399999_aux1,,
'''

from newecm import *
from upload import *
from collector import *

SUPPORT='EnerSave'
CLASS = 'EnerSaveProcessor'

class Constants:
# EnerSave defaults
#   Minimum upload interval is 60 seconds.
#   Recommended sampling interval is 2 to 30 seconds.
# the map is a comma-delimited list of channel,description,type tuples
#   311111_ch1,living room,2,311112_ch2,solar,1,311112_aux4,kitchen,2
  ES_URL           = 'http://data.myenersave.com/fetcher/data'
  ES_UPLOAD_PERIOD = MINUTE
  ES_TIMEOUT       = 60 # seconds
  ES_TOKEN         = ''
  ES_MAP           = ''
  ES_DEFAULT_TYPE  = 2

class EnerSaveProcessor(UploadProcessor):
    INSERT = True
    def __init__(self, *args, **kwargs):
        super(EnerSaveProcessor, self).__init__(*args, **kwargs)
        self.url     = kwargs.get('es_url')   or ES_URL
        self.token   = kwargs.get('es_token') or ES_TOKEN
        self.map_str = kwargs.get('es_map')   or ES_MAP
        self.upload_period = ES_UPLOAD_PERIOD
        self.timeout = ES_TIMEOUT

        infmsg('ES: upload period: %d' % self.upload_period)
        infmsg('ES: url: %s' % self.url)
        infmsg('ES: token: %s' % self.token)
        infmsg('ES: map: %s' % self.map_str)

    def setup(self):
        if not (self.url and self.token):
            print 'EnerSave Error: Insufficient parameters'
            if not self.url:
                print '  No URL'
            if not self.token:
                print '  No token'
            sys.exit(1)

        self.map = self.tuples2dict(self.map_str)

    def tuples2dict(self, s):
        items = s.split(',')
        m = {}
        for k,d,t in zip(items[::3], items[1::3], items[2::3]):
            m[k] = { 'desc':d, 'type':t }
        return m

    def process_calculated(self, ecm_serial, packets):
        sensors = {}
        readings = {}
        for p in packets:
            if self.map:
                for c in ECM1240_CHANNELS:
                    key = ecm_serial + '_' + c
                    if key in self.map:
                        tpl = self.map[key]
                        dev_id = obfuscate_serial(ecm_serial) + '_' + c
                        dev_type = tpl['type'] or ES_DEFAULT_TYPE
                        dev_desc = tpl['desc'] or c
                        sensors[dev_id] = { 'type':dev_type, 'desc':dev_desc }
                        if not dev_id in readings:
                            readings[dev_id] = []
                            readings[dev_id].append('<energy time="%d" wh="%.4f"/>' %
                                                    (p['time_created'], p[c+'_wh']))
            else:
                for c in ECM1240_CHANNELS:
                    dev_id = obfuscate_serial(ecm_serial) + '_' + c
                    dev_type = ES_DEFAULT_TYPE
                    dev_desc = c
                    sensors[dev_id] = { 'type':dev_type, 'desc':dev_desc }
                    if not dev_id in readings:
                        readings[dev_id] = []
                        readings[dev_id].append('<energy time="%d" wh="%.4f"/>' %
                                                (p['time_created'], p[c+'_wh']))
        s = []
        for key in sensors:
            s.append('<sensor id="%s" type="%s" description="%s">' %
                     (key, sensors[key]['type'], sensors[key]['desc']))
            s.append(''.join(readings[key]))
            s.append('</sensor>')
        if len(s):
            s.insert(0, '<?xml version="1.0" encoding="UTF-8" ?>')
            s.insert(1, '<upload>')
            s.append('</upload>')
            self._urlopen(ecm_serial, self.url, ''.join(s))
            # FIXME: check for server response
        self.mark_upload_complete(ecm_serial)

    def _handle_urlopen_error(self, e, sn, url, payload):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  ECM:   ' + sn,
                        '\n  URL:   ' + url,
                        '\n  token: ' + self.token,
                        '\n  data:  ' + payload,]))

    def _create_request(self, url):
        req = super(EnerSaveProcessor, self)._create_request(url)
        req.add_header("Content-Type", "application/xml")
        req.add_header("Token", self.token)
        return req

    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'EnerSave options')
      group.add_option('--enersave', action='store_true', dest='enersave_out', default=False, help='upload data using EnerSave API')
      group.add_option('--es-token', help='token')
      group.add_option('--es-url', help='URL')
      group.add_option('--es-map', help='channel-to-device mapping')
      parser.add_option_group(group)

    @staticmethod
    def is_enabled(options):
      return options.enersave_out

    @staticmethod
    def help():
      print '  --enersave           upload to EnerSave'

    @staticmethod
    def make(options, procs):
      if options.enersave_out:
          procs.append(**vars(options))
