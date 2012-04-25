'''
PeoplePower Configuration:

1) create an account
2) obtain an activation key
3) register a hub to obtain an authorization token for that hub
4) indicate which ECM channels should be recorded by configuring devices
    associated with the hub

1) Find an iPhone or droid, run the PeoplePower app, register for an account.

2) When you register for an account, enter TED as the device type.  An
activation key will be sent to the email account used during registration.

3) Use the activation key to obtain a device authorization token.  Create an
XML file with the activation key, a 'hub ID', and a device type.  One way to do
this is to treat the ecmread script as the hub and each channel of each ECM
as a device.  Use any number for the hub ID, and a device type 1004 (TED-5000).

For example, create the file req.xml with the following contents:

<?xml version="1.0" encoding="UTF-8"?>
<ted5000Activation>
  <Gateway>1</Gateway>
  <Unique>activationKey</Unique>
</ted5000Activation>

Send the file using HTTP POST to the hub activation URL:

curl -X POST -d @req.xml http://esp.peoplepowerco.com/espapi/rest/activated

You should get a response with resultCode 0 and a deviceAuthToken.  The
response also contains pieces of the URL that you should use for subsequent
configuration.  For example:

<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<response>
  <resultCode>0</resultCode>
  <host>esp.peoplepowerco.com</host>
  <useSSL>true</useSSL>
  <port>8443</port>
  <uri>deviceio/ml</uri>
  <deviceAuthToken>XXXXX</deviceAuthToken>
</response>

<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ted5000ActivationResponse>
  <PostServer>esp.peoplepowerco.com</PostServer>
  <UseSSL>true</UseSSL>
  <PostPort>8443</PostPort>
  <PostURL>/deviceio/ted</PostURL>
  <SSLKey></SSLKey>
  <AuthToken>7Vc1La3Ca-esy4COjTQ</AuthToken>
  <PostRate>1</PostRate>
</ted5000ActivationResponse>

which results in this URL:

https://esp.peoplepowerco.com:8443/deviceio/ted

4) Add each channel of each ECM as a new ppco device.  The ecmread script
will configure the device definitions, but it requires a map of ECM channels
to device names.  This map also indicates the channel(s) from which data
should be uploaded.

For example, via configuration file:

[peoplepower]
peoplepower_out=true
pp_url=https://esp.peoplepowerco.com:8443/deviceio/ml
pp_token=XXX
pp_hub_id=YYY
pp_map=399999_ch1,999c1,399999_aux2,999a2

Additional notes for PeoplePower:

Use the device type 1005, which is a generic TED MTU.  Create an arbitrary
deviceId for each ECM channel that will be tracked.  Apparently the deviceId
must contain hex characters, so use c1 and c2 for channels 1 and 2 and use aN
for the aux.  The seq field is a monotonically increasing nonce.

<?xml version="1" encoding="UTF-8"?>
<h2s seq="1" hubId="1" ver="2">
  <add deviceId="3XXXXXc1" deviceType="1005" />
  <add deviceId="3XXXXXc2" deviceType="1005" />
  <add deviceId="3XXXXXa1" deviceType="1005" />
  <add deviceId="3XXXXXa2" deviceType="1005" />
  <add deviceId="3XXXXXa3" deviceType="1005" />
</h2s>

Send the file to the URL received in the activation response.

curl -H "Content-Type: text/xml" -H "PPCAuthorization: esp token=TOKEN" -d @add.xml https://esp.peoplepowerco.com:8443/deviceio/ml

To get a list of devices that ppco recognizes:
  curl https://esp.peoplepowerco.com/espapi/rest/deviceTypes
'''

from newecm import *
from upload import *
from collector import *
from config import *

SUPPORT='PeoplePower'
CLASS='PeoplePowerProcessor'

class Constants:
# PeoplePower defaults
#   http://developer.peoplepowerco.com/docs
#   Recommended upload period is 15 minutes.
# the map is a comma-delimited list of channel,meter pairs.  for example:
#   311111_ch1,1111c1,311112_ch1,1112c1,311112_aux4,1112a4
  PPCO_URL            = 'https://esp.peoplepowerco.com:8443/deviceio/ml'
#PPCO_UPLOAD_PERIOD  = 15 * MINUTE
  PPCO_UPLOAD_PERIOD  = MINUTE
  PPCO_TIMEOUT        = 15 # seconds
  PPCO_TOKEN          = ''
  PPCO_HUBID          = 1
  PPCO_MAP            = ''
  PPCO_FIRST_NONCE    = 1
  PPCO_DEVICE_TYPE    = 1005

class PeoplePowerProcessor(UploadProcessor):
    INSERT = True
    def __init__(self, *args, **kwargs):
        super(PeoplePowerProcessor, self).__init__(*args, **kwargs)
        self.url      = kwargs.get('pp_url')    or PPCO_URL
        self.token    = kwargs.get('pp_token')  or PPCO_TOKEN
        self.hub_id   = kwargs.get('pp_hub_id') or PPCO_HUBID
        self.map_str  = kwargs.get('pp_map')    or PPCO_MAP
        self.nonce    = PPCO_FIRST_NONCE
        self.dev_type = PPCO_DEVICE_TYPE
        self.upload_period = PPCO_UPLOAD_PERIOD
        self.timeout = PPCO_TIMEOUT

        infmsg('PP: upload period: %d' % self.upload_period)
        infmsg('PP: url: %s' % self.url)
        infmsg('PP: token: %s' % self.token)
        infmsg('PP: hub id: %s' % self.hub_id)
        infmsg('PP: map: %s' % self.map_str)

    def setup(self):
        if not (self.url and self.token and self.hub_id and self.map_str):
            print 'PeoplePower Error: Insufficient parameters'
            if not self.url:
                print '  No URL'
            if not self.token:
                print '  No token'
            if not self.hub_id:
                print '  No hub ID'
            if not self.map_str:
                print '  No mapping between ECM channels and PeoplePower devices'
            sys.exit(1)

        self.map = pairs2dict(self.map_str)
        if not self.map:
            print 'PeoplePower Error: cannot determine channel-meter map'
            sys.exit(1)

        self.add_devices()

    def process_calculated(self, ecm_serial, packets):
        s = []
        for p in packets:
            for c in ECM1240_CHANNELS:
                key = ecm_serial + '_' + c
                if key in self.map:
                    ts = time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                       time.gmtime(p['time_created']))
                    s.append('<measure deviceId="%s" deviceType="%s" timestamp="%s">' % (self.map[key], self.dev_type, ts))
                    s.append('<param name="power" units="W">%1.4f</param>' %
                             p[c+'_watts'])
                    s.append('<param name="energy" units="Wh">%1.4f</param>' %
                             p[c+'_wh'])
                    s.append('</measure>')
        if len(s):
            result = self._urlopen(ecm_serial, self.url, s)
            resp = result.read()
            if not resp or resp.find('ACK') == -1:
                wrnmsg('PP: upload failed: %s' % resp)
        self.mark_upload_complete(ecm_serial)

    def add_devices(self):
        s = []
        for key in self.map.keys():
            s.append('<add deviceId="%s" deviceType="%s" />' %
                     (self.map[key], self.dev_type))
        if len(s):
            result = self._urlopen('setup', self.url, s)
            resp = result.read()
            if not resp or resp.find('ACK') == -1:
                wrnmsg('PP: add devices failed: %s' % resp)

    def _urlopen(self, sn, url, s):
        s.insert(0, '<?xml version="1.0" encoding="UTF-8" ?>')
        s.insert(1, '<h2s ver="2" hubId="%s" seq="%d">' % 
                 (self.hub_id, self.nonce))
        s.append('</h2s>')
        result = super(PeoplePowerProcessor, self)._urlopen(sn, url, ''.join(s))
        self.nonce += 1
        return result

    def _handle_urlopen_error(self, e, sn, url, payload):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  ECM:    ' + sn,
                        '\n  URL:    ' + url,
                        '\n  token:  ' + self.token,
                        '\n  hub ID: ' + self.hub_id,
                        '\n  data:   ' + payload,]))

    def _create_request(self, url):
        req = super(PeoplePowerProcessor, self)._create_request(url)
        req.add_header("Content-Type", "text/xml")
        req.add_header("PPCAuthorization", "esp token=%s" % self.token)
        return req


    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'PeoplePower options')
      group.add_option('--peoplepower', action='store_true', dest='peoplepower_out', default=False, help='upload data using PeoplePower API')
      group.add_option('--pp-token', help='auth token')
      group.add_option('--pp-hub-id', help='hub ID')
      group.add_option('--pp-url', help='URL')
      group.add_option('--pp-map', help='channel-to-device mapping')
      parser.add_option_group(group)

    @staticmethod
    def is_enabled(options):
      return options.peoplepower_out

    @staticmethod
    def help():
      print '  --peoplepower        upload to PeoplePower'

    @staticmethod
    def make(options, procs):
      if options.peoplepower_out:
          procs.append(PeoplePowerProcessor(**vars(options)))
