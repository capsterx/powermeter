'''
ThingSpeak Configuration:

1) create an account
2) create a thingspeak channel for each ECM
3) create a field for each ECM channel

Create an account at the ThingSpeak web site.

Create a ThingSpeak channel for each ECM.  Obtain the token (write api key) for
each channel.

Create a field for each ECM data from which you will upload data.

By default, data from all channels on all ECMs will be uploaded.  The channel
ID and token must be specified for each ECM.  By default, the ECM channels
will be uploaded to fields 1-7.

For example, this configuration will upload all data from ECM with serial
399999 to thingspeak channel with token 12345 and from ECM with serial
399998 to thingspeak channel with token 12348.

[thingspeak]
thingspeak_out=true
ts_tokens=399999,12345,399998,12348

This configuration will upload only ch1 from 399999 to field 3 and aux5 from
399998 to field 8:

[thingspeak]
thingspeak_out=true
ts_tokens=399999,12345,399998,12348
ts_fields=399999_ch1,3,399998_aux5,8
'''

from newecm import *
from upload import *
from collector import *
from config import *

SUPPORT='ThingSpeak'
CLASS = 'ThingSpeakProcessor'

class Constants:
# thingspeak defaults
#   Uploads are limited to no more than every 15 seconds per channel.
  TS_URL           = 'http://api.thingspeak.com/update'
  TS_UPLOAD_PERIOD = MINUTE
  TS_TIMEOUT       = 15 # seconds
  TS_TOKENS        = ''
  TS_FIELDS        = ''

class ThingSpeakProcessor(UploadProcessor):
    INSERT = True
    def __init__(self, *args, **kwargs):
        super(ThingSpeakProcessor, self).__init__(*args, **kwargs)
        self.url        = kwargs.get('ts_url')    or TS_URL
        self.tokens_str = kwargs.get('ts_tokens') or TS_TOKENS
        self.fields_str = kwargs.get('ts_fields') or TS_FIELDS
        self.upload_period = TS_UPLOAD_PERIOD
        self.timeout = TS_TIMEOUT

        infmsg('TS: upload period: %d' % self.upload_period)
        infmsg('TS: url: ' + self.url)
        infmsg('TS: tokens: ' + self.tokens_str)
        infmsg('TS: fields: ' + self.fields_str)

    def setup(self):
        if not (self.url and self.tokens_str):
            print 'ThingSpeak Error: Insufficient parameters'
            if not self.url:
                print '  No URL'
            if not self.tokens_str:
                print '  No tokens'
            sys.exit(1)

        self.tokens = pairs2dict(self.tokens_str)
        self.fields = pairs2dict(self.fields_str)

    def process_calculated(self, ecm_serial, packets):
        if ecm_serial in self.tokens:
            token = self.tokens[ecm_serial]
            for p in packets:
                s = []
                for idx,c in enumerate(ECM1240_CHANNELS):
                    key = ecm_serial + '_' + c
                    if not self.fields:
                        s.append('&field%d=%d' % (idx, p[c+'_watts']))
                    elif key in self.fields:
                        s.append('&field%s=%d' % (self.fields[key], p[c+'_watts']))
                if len(s):
                    s.insert(0, 'key=%s' % token)
#                    ts = time.strftime('%Y-%m-%dT%H:%M:%SZ',
#                                       time.gmtime(p['time_created']))
#                    s.insert(1, '&datetime=%s' % ts)
                    result = self._urlopen(ecm_serial, self.url, ''.join(s))
                    if result:
                        resp = result.read()
                        if resp == 0:
                            wrnmsg('TS: upload failed for %s: %s' % (ecm_serial, resp))                        
                    else:
                        wrnmsg('TS: upload failed for %s' % ecm_serial)
        else:
            wrnmsg('TS: no token defined for %s' % ecm_serial)
        self.mark_upload_complete(ecm_serial)

    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'ThingSpeak options')
      group.add_option('--thingspeak', action='store_true', dest='thingspeak_out', default=False, help='upload data using ThingSpeak API')
      group.add_option('--ts-url', help='URL')
      group.add_option('--ts-tokens', help='ECM-to-ID/token mapping')
      group.add_option('--ts-fields', help='channel-to-field mapping')
      parser.add_option_group(group)

    @staticmethod
    def is_enabled(options):
      return options.thingspeak_out

    @staticmethod
    def help():
      print '  --thingspeak         upload to ThingSpeak'

    @staticmethod
    def make(options, procs):
      if options.thingspeak_out:
          procs.append(ThingSpeakProcessor(**vars(options)))
