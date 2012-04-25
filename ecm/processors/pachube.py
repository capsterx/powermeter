'''
Pachube Configuration:

1) create an account
2) obtain API key
3) create a feed

Create an account at the Pachube web site.

Obtain the API key from the Pachube web site.

Create a feed at the Pachube web site, or using curl as described at pachube.

By default, data from every channel from every ECM will be uploaded to a single
pachube feed.

[pachube]
pachube_out=true
pbe_token=XXXXXX
pbe_feed=3000
'''

from newecm import *
from upload import *
from collector import *

SUPPORT='Pachube'
CLASS='PachubeProcessor'

class Constants:
# pachube defaults
  PBE_URL           = 'http://api.pachube.com/v2/feeds'
  PBE_UPLOAD_PERIOD = MINUTE
  PBE_TIMEOUT       = 15 # seconds
  PBE_TOKEN         = ''
  PBE_FEED          = ''

class PachubeProcessor(UploadProcessor):
    INSERT = True
    def __init__(self, *args, **kwargs):
        super(PachubeProcessor, self).__init__(*args, **kwargs)
        self.url     = kwargs.get('pbe_url')   or PBE_URL
        self.token   = kwargs.get('pbe_token') or PBE_TOKEN
        self.feed    = kwargs.get('pbe_feed')  or PBE_FEED
        self.upload_period = PBE_UPLOAD_PERIOD
        self.timeout = PBE_TIMEOUT

        infmsg('PBE: upload period: %d' % self.upload_period)
        infmsg('PBE: url: ' + self.url)
        infmsg('PBE: token: ' + self.token)
        infmsg('PBE: feed: ' + self.feed)

    def setup(self):
        if not (self.url and self.token and self.feed):
            print 'Pachube Error: Insufficient parameters'
            if not self.url:
                print '  A URL is required'
            if not self.url:
                print '  A token is required'
            if not self.feed:
                print '  A feed is required'
            sys.exit(1)

    def process_calculated(self, ecm_serial, packets):
        for p in packets:
            data = {
                'version':'1.0.0',
                'datastreams':[]
                }
#            ts = time.strftime('%Y-%m-%dT%H:%M:%SZ',
#                               time.gmtime(p['time_created']))
            for idx,c in enumerate(ECM1240_CHANNELS):
                dpkey = obfuscate_serial(ecm_serial) + '_' + c
#                dp = {'id':dpkey, 'at':ts, 'value':p[c+'_watts']}
                dp = {'id':dpkey, 'current_value':p[c+'_watts']}
                data['datastreams'].append(dp)
            if len(data['datastreams']):
                url = '%s/%s' % (self.url, self.feed)
                result = self._urlopen(ecm_serial, url, json.dumps(data))
        # FIXME: need better error handling here
        self.mark_upload_complete(ecm_serial)

    def _create_request(self, url):
        req = super(PachubeProcessor, self)._create_request(url)
        req.add_header('X-PachubeApiKey', self.token)
        req.get_method = lambda: 'PUT'
        return req

    def _handle_urlopen_error(self, e, sn, url, payload):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  ECM:   ' + sn,
                        '\n  URL:   ' + url,
                        '\n  token: ' + self.token,
                        '\n  data:  ' + payload,]))

    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'Pachube options')
      group.add_option('--pachube', action='store_true', dest='pachube_out', default=False, help='upload data using Pachube API')
      group.add_option('--pbe-url', help='URL')
      group.add_option('--pbe-token', help='token')
      group.add_option('--pbe-feed', help='feed')
      parser.add_option_group(group)

    @staticmethod
    def is_enabled(options):
      return options.pachube_out

    @staticmethod
    def help():
      print '  --pachube            upload to Pachube'

    @staticmethod
    def make(options, procs):
      if options.pachube_out:
          procs.append(PachubeProcessor(**vars(options)))
