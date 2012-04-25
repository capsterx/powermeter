'''
Smart Energy Groups Configuration:

1) create an account
2) create a site
3) obtain the site token
4) optionally indicate which ECM channels should be recorded and assign labels

Create an account at the smart energy groups web site.

Create a site at the smart energy groups web site.

Obtain the site token at the smart energy groups web site.

Create devices on the smart energy groups web site.  Create one device per ECM.
For each device create 14 streams - one power stream and one energy stream for
each ECM channel.  Define a node name for each device based on the following.

This can be done by going to My sites/Tools/Discoveries and then run ecmread.  
After the first posting, it should add all your devices correctly.

By default, data from all channels on all ECM will be uploaded.  The node
name is the obfuscated ECM serial number, for example XXX123 for the serial
number 355123. The stream name is p_* or e_* for each channel for power or
energy, respectively. For example, p_ch1, e_ch1, p_aux1, e_aux1

To upload only a portion of the data, or to use names other than the defaults,
specify a map via command line or configuration file.  It is easiest to use the
default node names then add longer labels at the Smart Energy Groups web site.

For example, here is a configuration that uploads data only from ch1 and aux2
from ECM 399999, using node names p_ch1, e_ch1, p_lighting, and e_lighting.

[smartenergygroups]
smartenergygroups_out=true
seg_token=XXX
seg_map=399999_ch1,,399999_aux2,lighting
'''

from newecm import *
from upload import *
from collector import *
from config import *

SUPPORT='Smart Energy Groups'
CLASS='SmartEnergyGroupsProcessor'

class Constants:
# smart energy groups defaults
# the map is a comma-delimited list of channel,meter pairs.  for example:
#   311111_ch1,living room,311112_ch1,parlor,311112_aux4,kitchen
  SEG_URL           = 'http://api.smartenergygroups.com/api_sites/stream'
  SEG_UPLOAD_PERIOD = MINUTE
  SEG_TIMEOUT       = 15 # seconds
  SEG_TOKEN         = ''
  SEG_MAP           = ''

class SmartEnergyGroupsProcessor(UploadProcessor):
    INSERT = True
    def __init__(self, *args, **kwargs):
        super(SmartEnergyGroupsProcessor, self).__init__(*args, **kwargs)
        self.url      = kwargs.get('seg_url')    or SEG_URL
        self.token    = kwargs.get('seg_token')  or SEG_TOKEN
        self.map_str  = kwargs.get('seg_map')    or SEG_MAP
        self.upload_period = SEG_UPLOAD_PERIOD
        self.timeout = SEG_TIMEOUT

        infmsg('SEG: upload period: %d' % self.upload_period)
        infmsg('SEG: url: ' + self.url)
        infmsg('SEG: token: ' + self.token)
        infmsg('SEG: map: ' + self.map_str)

    def setup(self):
        if not (self.url and self.token):
            print 'SmartEnergyGroups Error: Insufficient parameters'
            if not self.url:
                print '  No URL'
            if not self.token:
                print '  No token'
            sys.exit(1)

        self.map = pairs2dict(self.map_str)
        self.urlopener = urllib2.build_opener(urllib2.HTTPHandler)

    def process_calculated(self, ecm_serial, packets):
        osn = obfuscate_serial(ecm_serial)
        for p in packets:
            s = []
            if self.map:
                for idx,c in enumerate(ECM1240_CHANNELS):
                    key = ecm_serial + '_' + c
                    if key in self.map:
                        meter = self.map[key] or c # str(idx+1)
                        s.append('(p_%s %1.4f)' % (meter,p[c+'_watts']))
                        s.append('(e_%s %1.4f)' % (meter,p[c+'_wh']))
            else:
                for idx,c in enumerate(ECM1240_CHANNELS):
                    meter = c # str(idx+1)
                    s.append('(p_%s %1.4f)' % (meter,p[c+'_watts']))
                    s.append('(e_%s %1.4f)' % (meter,p[c+'_wh']))
            if len(s):
                ts = time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                   time.gmtime(p['time_created']))
                s.insert(0, 'data_post=(site %s ' % self.token)
                s.insert(1, '(node %s %s ' % (osn, ts))
                s.append(')')
                s.append(')')
                result = self._urlopen(ecm_serial, self.url, ''.join(s))
                resp = result.read()
                resp = resp.replace('\n', '')
                if not resp == '(status ok)':
                    wrnmsg('SEG: upload failed for %s: %s' % (ecm_serial, resp))
        self.mark_upload_complete(ecm_serial)

    def _handle_urlopen_error(self, e, sn, url, payload):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  ECM:   ' + sn,
                        '\n  URL:   ' + url,
                        '\n  token: ' + self.token,
                        '\n  data:  ' + payload,]))

    def _create_request(self, url):
        req = super(SmartEnergyGroupsProcessor, self)._create_request(url)
        req.get_method = lambda: 'PUT'
        return req


    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'Smart Energy Groups options')
      group.add_option('--smartenergygroups', action='store_true', dest='smartenergygroups_out', default=False, help='upload data using SmartEnergyGroups API')
      group.add_option('--seg-token', help='token')
      group.add_option('--seg-url', help='URL')
      group.add_option('--seg-map', help='channel-to-device mapping')
      parser.add_option_group(group)

    @staticmethod
    def is_enabled(options):
      return options.smartenergygroups_out

    @staticmethod
    def help():
      print '  --smartenergygroups  upload to SmartEnergyGroups'

    @staticmethod
    def make(options, procs):
      if options.smartenergygroups_out:
          procs.append(SmartEnergyGroupsProcessor(**vars(options)))
