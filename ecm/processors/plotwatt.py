'''
PlotWatt Configuration:

1) register for an account
2) obtain a house ID and an API key
3) configure meters that correspond to ECM channels

First register for a plotwatt account at www.plotwatt.com.  You should receive
a house ID and an api key.  Then configure 'meters' at the plotwatt web site
that correspond to channels on each ECM.

Using curl to create 2 meters looks something like this:

curl -d "number_of_new_meters=2" http://API_KEY:@plotwatt.com/api/v2/new_meters

Using curl to list existing meters looks something like this:

curl http://API_KEY:@plotwatt.com/api/v2/list_meters

Use the meter identifiers provided by plotwatt when uploading data, with each
meter associated with a channel/aux of an ECM.  For example, to upload data
from ch1 and ch2 from ecm serial 399999 to meters 1000 and 1001, respectively,
use a configuration like this:

[plotwatt]
plotwatt_out=true
pw_house_id=XXXX
pw_api_key=XXXXXXXXXXXXXXXX
pw_map=399999_ch1,1000,399999_ch2,1001
'''

from newecm import *
from upload import *
from collector import *
from config import *

SUPPORT='PlotWatt'
CLASS='PlotWattProcessor'

class Constants:
# PlotWatt defaults
#   https://plotwatt.com/docs/api
#   Recommended upload period is one minute to a few minutes.  Recommended
#   sampling as often as possible, no faster than once per second.
# the map is a comma-delimited list of channel,meter pairs.  for example:
#   311111_ch1,1234,311112_ch1,1235,311112_aux4,1236
  PLOTWATT_BASE_URL      = 'http://plotwatt.com'
  PLOTWATT_UPLOAD_URL    = '/api/v2/push_readings'
  PLOTWATT_UPLOAD_PERIOD = MINUTE
  PLOTWATT_TIMEOUT       = 15 # seconds
  PLOTWATT_MAP           = ''
  PLOTWATT_HOUSE_ID      = ''
  PLOTWATT_API_KEY       = ''

class PlotWattProcessor(UploadProcessor):
    INSERT = True
    def __init__(self, *args, **kwargs):
        super(PlotWattProcessor, self).__init__(*args, **kwargs)
        self.api_key  = kwargs.get('pw_api_key')  or PLOTWATT_API_KEY
        self.house_id = kwargs.get('pw_house_id') or PLOTWATT_HOUSE_ID
        self.map_str  = kwargs.get('pw_map')      or PLOTWATT_MAP
        self.url = PLOTWATT_BASE_URL + PLOTWATT_UPLOAD_URL
        self.upload_period = PLOTWATT_UPLOAD_PERIOD
        self.timeout = PLOTWATT_TIMEOUT

        infmsg('PW: upload period: %d' % self.upload_period)
        infmsg('PW: url: %s' % self.url)
        infmsg('PW: api key: %s' % self.api_key)
        infmsg('PW: house id: %s' % self.house_id)
        infmsg('PW: map: %s' % self.map_str)

    def setup(self):
        if not (self.api_key and self.house_id and self.map_str):
            print 'PlotWatt Error: Insufficient parameters'
            if not self.api_key:
                print '  No API key'
            if not self.house_id:
                print '  No house ID'
            if not self.map_str:
                print '  No mapping between ECM channels and PlotWatt meters'
            sys.exit(1)

        self.map = pairs2dict(self.map_str)
        if not self.map:
            print 'PlotWatt Error: cannot determine channel-meter map'
            sys.exit(1)

    def process_calculated(self, ecm_serial, packets):
        s = []
        for p in packets:
            for c in ECM1240_CHANNELS:
                key = ecm_serial + '_' + c
                if key in self.map:
                    meter = self.map[key]
                    # format for each meter is: meter-id,kW,gmt-timestamp
                    s.append("%s,%1.4f,%d" %
                             (meter, p[c+'_watts']/1000, p['time_created']))
        if len(s):
            self._urlopen(ecm_serial, self.url, ','.join(s))
            # FIXME: check for server response
        self.mark_upload_complete(ecm_serial)

    def _handle_urlopen_error(self, e, sn, url, payload):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  ECM:      ' + sn,
                        '\n  URL:      ' + url,
                        '\n  API key:  ' + self.api_key,
                        '\n  house ID: ' + self.house_id,
                        '\n  data:     ' + payload,]))

    def _create_request(self, url):
        req = super(PlotWattProcessor, self)._create_request(url)
        req.add_header("Content-Type", "text/xml")
        b64s = base64.encodestring('%s:%s' % (self.api_key, ''))[:-1]
        req.add_header("Authorization", "Basic %s" % b64s)
        return req

    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'PlotWatt options')
      group.add_option('--plotwatt', action='store_true', dest='plotwatt_out', default=False, help='upload data using PlotWatt API')
      group.add_option('--pw-house-id', help='house ID')
      group.add_option('--pw-api-key', help='API key')
      group.add_option('--pw-map', help='channel-to-meter mapping')
      parser.add_option_group(group)

    @staticmethod
    def is_enabled(options):
      return options.plotwatt_out

    @staticmethod
    def help():
      print '  --plotwatt           upload to PlotWatt'

    @staticmethod
    def make(options, procs):
      if options.plotwatt_out:
          procs.append(PlotWattProcessor(**vars(options)))
