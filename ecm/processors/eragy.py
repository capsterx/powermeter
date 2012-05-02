'''
Eragy Configuration:

1) register power sensor at the eragy web site
2) obtain a token
3) create an account

The Eragy web site only knows about TED, Blueline, and eGauge devies.  Register
as a TED5000 power sensor.  Eragy will provide a URL and token.  Use curl to
enter the registration for each ECM.  Create a file req.xml with the request:

<ted5000Activation>
  <Gateway>XXX</Gateway>
  <Unique>TOKEN</Unique>
</ted5000Activation>

where XXX is an arbitrary gateway ID and TOKEN is the token issued by Eragy.
If you have only one ECM, use the ECM serial number as the gateway ID.  If
you have multiple ECM, use one of the ECM serial numbers or just pick a number.

Send the request using curl:

curl -X POST -d @req.xml http://d.myeragy.com/energyremote.aspx

The server will respond with something like this:

<ted5000ActivationResponse>
  <PostServer>d.myeragy.com</PostServer>
  <UseSSL>false</UseSSL>
  <PostPort>80</PostPort>
  <PostURL>/energyremote.aspx</PostURL>
  <SSLKey></SSLKey>
  <AuthToken>TOKEN</AuthToken>
  <PostRate>1</PostRate>
</ted5000ActivationResponse>

At the eragy web site, click the 'find my sensor' button.  Eragy assumes that
the energy sensor will immediately start sending data to eragy, so start
running ecmread.

On the eragy web site, continue and create an account.  Eragy will email to
you an account password which you must enter to complete the account.  Then
configure the account with a name, timezone, etc. and password.

The eragy configuration would be:

[eragy]
eragy_out=true
eg_token=TOKEN
eg_gateway_id=XXX
'''

from newecm import *
from upload import *
from collector import *

SUPPORT='Eragy'
CLASS='EragyProcessor'

class Constants:
# eragy defaults
  ERAGY_URL           = 'http://d.myeragy.com/energyremote.aspx'
  ERAGY_UPLOAD_PERIOD = 5 * MINUTE
  ERAGY_TIMEOUT       = 15 # seconds
  ERAGY_GATEWAY_ID    = ''
  ERAGY_TOKEN         = ''

class EragyProcessor(UploadProcessor):
    INSERT = True
    def __init__(self, *args, **kwargs):
        super(EragyProcessor, self).__init__(*args, **kwargs)
        self.url        = kwargs.get('eg_url')        or ERAGY_URL
        self.gateway_id = kwargs.get('eg_gateway_id') or ERAGY_GATEWAY_ID
        self.token      = kwargs.get('eg_token')      or ERAGY_TOKEN
        self.upload_period = ERAGY_UPLOAD_PERIOD
        self.timeout = ERAGY_TIMEOUT

        infmsg('EG: upload period: %d' % self.upload_period)
        infmsg('EG: url: ' + self.url)
        infmsg('EG: gateway ID: ' + self.gateway_id)
        infmsg('EG: token: ' + self.token)

    def setup(self):
        if not (self.url and self.gateway_id and self.token):
            print 'Eragy Error: Insufficient parameters'
            if not self.url:
                print '  No URL'
            if not self.gateway_id:
                print '  No gateway ID'
            if not self.token:
                print '  No token'
            sys.exit(1)
        self.last_watts = {}
        self.last_publish_times = {}
        self.sns = {}
    
    def process_compiled(self, packet, packet_buffer):
        sn = getserial(packet)
        self.sns[sn] = True
        if not self.time_to_upload(sn):
            return
        total_packets = {}
        for sn in self.sns:
          packets = []
          data = packet_buffer.data_over(sn, self.upload_period)
          dbgmsg('%d packets to process' % len(data))
          for a,b in zip(data[0:], data[1:]):
              packets.append(calculate(b[1],a[1]))
          total_packets[sn] = packets
        self.process_calculated(total_packets)

    def process_calculated(self, total_packets):
        time_samples = {}
        times = {}
        for ecm_serial, packets in total_packets.iteritems():
          osn = obfuscate_serial(ecm_serial)
          for p in packets:
            if p['time_created'] - self.last_publish_times.get(ecm_serial, 0) >= 59:
              for idx,c in enumerate(ECM1240_CHANNELS):
                key = osn + '_' + c
                if not key in times:
                  times[key] = []
                watts = p[c+'_wh'] * 1000
  	        times[key].append('<cumulative timestamp="%s" watts="%d"/>' % (p['time_created'], watts))
              self.last_publish_times[ecm_serial] = p['time_created']
        s = []
        for key, ts in times.iteritems():
          s.append('<MTU ID="%s">' % (key))
          for t in ts:
            s.append(t)
          s.append('</MTU>')
        if len(s):
            s.insert(0, '<ted5000 GWID="%s" auth="%s">' %
                     (self.gateway_id, self.token))
            s.append('</ted5000>')
            result = self._urlopen(ecm_serial, self.url, ''.join(s))
            resp = result.read()
            if not resp == '<xml>0</xml>':
                wrnmsg('EG: upload failed for %s: %s' % (ecm_serial, resp))
        for ecm_serial in self.sns:
          self.mark_upload_complete(ecm_serial)

    def _handle_urlopen_error(self, e, sn, url, payload):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  ECM:   ' + sn,
                        '\n  URL:   ' + url,
                        '\n  token: ' + self.token,
                        '\n  data:  ' + payload,]))

    def _create_request(self, url):
        req = super(EragyProcessor, self)._create_request(url)
        req.add_header("Content-Type", "text/xml")
        return req

    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'Eragy options')
      group.add_option('--eragy', action='store_true', dest='eragy_out', default=False, help='upload data using Eragy API')
      group.add_option('--eg-gateway-id', help='gateway id')
      group.add_option('--eg-token', help='token')
      group.add_option('--eg-url', help='URL')
      parser.add_option_group(group)

    @staticmethod
    def is_enabled(options):
      return options.eragy_out

    @staticmethod
    def help():
      print '  --eragy              upload to Eragy'

    @staticmethod
    def make(options, procs):
      if options.eragy_out:
          procs.append(EragyProcessor(**vars(options)))
