'''
WattzOn Configuration:

1) register for an account
2) obtain an API key
3) configure devices that correspond to ECM channels

As of December 2011, it appears that WattzOn service is no longer available.
'''

from newecm import *
from upload import *
from collector import *
from config import *

SUPPORT = 'WattzOn'
CLASS = 'WattzOnProcessor'

class Constants:
# WattzOn defaults
# the map is a comma-delimited list of channel,meter pairs.  for example:
#   311111_ch1,living room,311112_ch1,parlor,311112_aux4,kitchen
  WATTZON_API_URL       = 'http://www.wattzon.com/api/2009-01-27/3'
  WATTZON_UPLOAD_PERIOD = MINUTE
  WATTZON_TIMEOUT       = 15 # seconds
  WATTZON_MAP     = ''
  WATTZON_API_KEY = 'apw6v977dl204wrcojdoyyykr'
  WATTZON_USER    = ''
  WATTZON_PASS    = ''

class WattzOnProcessor(UploadProcessor):
    INSERT = True
    def __init__(self, *args, **kwargs):
        super(WattzOnProcessor, self).__init__(*args, **kwargs)
        self.api_key  = kwargs.get('wo_api_key') or WATTZON_API_KEY
        self.username = kwargs.get('wo_user')    or WATTZON_USER
        self.password = kwargs.get('wo_pass')    or WATTZON_PASS
        self.map_str  = kwargs.get('wo_map')     or WATTZON_MAP
        self.upload_period = WATTZON_UPLOAD_PERIOD
        self.timeout = WATTZON_TIMEOUT

        infmsg('WO: upload period: %d' % self.upload_period)
        infmsg('WO: url: %s' % WATTZON_API_URL)
        infmsg('WO: api key: %s' % self.api_key)
        infmsg('WO: username: %s' % self.username)
        infmsg('WO: map: %s' % self.map_str)

    def setup(self):
        if not (self.api_key and self.username and self.password and self.map_str):
            print 'WattzOn Error: Insufficient parameters'
            if not self.api_key:
                print '  No API key'
            if not self.username:
                print '  No username'
            if not self.password:
                print '  No passord'
            if not self.map_str:
                print '  No mapping between ECM channels and WattzOn meters'
            sys.exit(1)

        self.map = pairs2dict(self.map_str)
        if not self.map:
            print 'WattzOn Error: cannot determine channel-meter map'
            sys.exit(1)

        p = urllib2.HTTPPasswordMgrWithDefaultRealm()
        p.add_password('WattzOn', WATTZON_API_URL, self.username,self.password)
        auth = urllib2.HTTPBasicAuthHandler(p)
        self.urlopener = urllib2.build_opener(auth)

    def process_calculated(self, ecm_serial, packets):
        for p in packets:
            for c in ECM1240_CHANNELS:
                key = ecm_serial + '_' + c
                if key in self.map:
                    meter = self.map[key]
                    ts = time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                       time.gmtime(p['time_created']))
                    result = self._make_call(meter, ts, p[c+'_watts'])
                    infmsg('WattzOn: %s [%s] magnitude: %s' %
                           (ts, meter, p[c+'_watts']))
                    dbgmsg('WattzOn: %s' % result.info())
        self.mark_upload_complete(ecm_serial)

    def handle(self, e):
        if type(e) == urllib2.HTTPError:
            errmsg(''.join(['HTTPError:  ' + str(e),
                            '\n  URL:      ' + e.geturl(),
                            '\n  username: ' + self.username,
                            '\n  password: ' + self.password,
                            '\n  API key:  ' + self.api_key,]))
            return True
        return super(WattzOnProcessor, self).handle(e)

    def _make_call(self, meter, timestamp, magnitude):
        data = {
            'updates': [
                {
                    'timestamp': timestamp,
                    'power': {
                        'magnitude': int(magnitude), # truncated by WattzOn API
                        'unit':  'W',
                        }
                    },
                ]
            }
        url = '%s/user/%s/powermeter/%s/upload.json?key=%s' % (
            WATTZON_API_URL,
            self.username,
            urllib.quote(meter),
            self.api_key
            )
        req = self._create_request(url)
        return self.urlopener.open(req, json.dumps(data), self.timeout)

    def _create_request(self, url):
        req = super(WattzOnProcessor, self)._create_request(url)
        req.add_header("Content-Type", "application/json")
        return req

    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'WattzOn options')
      group.add_option('--wattzon', action='store_true', dest='wattzon_out', default=False, help='upload data using WattzOn API')
      group.add_option('--wo-user', help='username')
      group.add_option('--wo-pass', help='password')
      group.add_option('--wo-api-key', help='API key')
      group.add_option('--wo-map', help='channel-to-meter mapping')
      parser.add_option_group(group)

    @staticmethod
    def is_enabled(options):
      return options.wattzon_out

    @staticmethod
    def help():
      print '  --wattzon            upload to WattzOn'

    @staticmethod
    def make(options, procs):
        if options.wattzon_out:
            procs.append(WattzOnProcessor(**vars(options)))
