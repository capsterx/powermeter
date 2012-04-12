#!/usr/bin/env python -u
'''PowerMeter Data Processor for Brultech ECM-1240.

Collect data from Brultech ECM-1240 power monitors.  Print the data, save the
data to database, or upload the data to a server.

Includes support for uploading to the following services:
  * MyEnerSave    * SmartEnergyGroups   * pachube        * WattzOn
  * PlotWatt      * PeoplePower         * thingspeak     * Eragy

Thanks to:
  Amit Snyderman <amit@amitsnyderman.com>
  bpwwer & tenholde from the cocoontech.com forums
  Kelvin Kakugawa
  brian jackson [http://fivejacksons.com/brian/]
  Marc MERLIN <marc_soft@merlins.org> - http://marc.merlins.org/
  Ben <ben@brultech.com>

Example usage:

ecmread.py --serial --serialport=/dev/ttyUSB0 -p

Example output:

2010/06/07 21:48:37: ECM ID: 355555
2010/06/07 21:48:37: Volts:                 120.90V
2010/06/07 21:48:37: Ch1 Amps:                0.25A
2010/06/07 21:48:37: Ch2 Amps:                3.24A
2010/06/07 21:48:37: Ch1 Watts:            -124.586KWh ( 1536W)
2010/06/07 21:48:37: Ch1 Positive Watts:    210.859KWh ( 1536W)
2010/06/07 21:48:37: Ch1 Negative Watts:    335.445KWh (    0W)
2010/06/07 21:48:37: Ch2 Watts:            -503.171KWh (    0W)
2010/06/07 21:48:37: Ch2 Positive Watts:      0.028KWh (    0W)
2010/06/07 21:48:37: Ch2 Negative Watts:    503.199KWh (    0W)
2010/06/07 21:48:37: Aux1 Watts:            114.854KWh (  311W)
2010/06/07 21:48:37: Aux2 Watts:             80.328KWh (  523W)
2010/06/07 21:48:37: Aux3 Watts:             13.014KWh (   35W)
2010/06/07 21:48:37: Aux4 Watts:              4.850KWh (    0W)
2010/06/07 21:48:37: Aux5 Watts:             25.523KWh (  137W)


How to specify options:

Options can be specified via command line, in a configuration file, or by
modifying constants in this file.  Use -h or --help to see the complete list
of command-line options.  The configuration file is INI format, with parameter
names corresponding to the command-line options.

Here are contents of a sample configuration file that listens for data on port
8083 then uploads only channel 1 and channel 2 data from ecm with serial number
311111 to plotwatt and enersave and saves to database:

[general]
serial_read = false
serial_port = COM1
ip_read = true
ip_port = 8083
ip_mode = server

[database]
db_out = true
db_host = localhost
db_user = ecmuser
db_passwd = ecmpass
db_database = ecm

[plotwatt]
plotwatt_out = true
pw_map = 311111_ch1,123,311111_ch2,124
pw_house_id = 1234
pw_api_key = 12345

[enersave]
enersave_out = true
es_map = 311111_ch1,kitchen,2,311111_ch2,solar panels,1


Data Collection:

Data can be collected by serial/usb or tcp/ip.  When collecting via serial/usb,
ecmread is always a 'client' to the serial/usb device - ecmread blocks until
data appear.  When collecting via tcp/ip, ecmread can act as either a server
or client.  When in server mode, ecmread blocks until a client connects.  When
in client mode, ecmread opens a connection to the server then blocks until data
have been read.


Database Configuration:

When saving data to database, this script writes power (watts) to a table.

Create the database 'ecm' by doing something like this:

mysql -u root -p
mysql> create database ecm;
mysql> grant usage on *.* to ecmuser@ecmclient identified by 'ecmpass';
mysql> grant all privileges on ecm.* to ecmuser@ecmclient;

Create the table 'ecm' by doing something like this:

mysql> create table ecm
    -> (id int primary key auto_increment,
    -> time_created int,
    -> ecm_serial int,
    -> volts float,
    -> ch1_amps float,
    -> ch2_amps float,
    -> ch1_w int,
    -> ch2_w int,
    -> aux1_w int,
    -> aux2_w int,
    -> aux3_w int,
    -> aux4_w int,
    -> aux5_w int);

If you do not want the database to grow, then do not create the 'id' primary
key, and make the ecm_serial the primary key without auto_increment.  In that
case the database is used for data transfer, not data capture.

With sqlite the database setup goes like this:

sqlite3 /path/to/database.db
sqlite> create table ecm (id int primary key, time_created int, ...


WattzOn Configuration:

1) register for an account
2) obtain an API key
3) configure devices that correspond to ECM channels

As of December 2011, it appears that WattzOn service is no longer available.


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

<request>
  <hubActivation>
    <activationKey>xxx:yyyyyyyyyyyyyyy</activationKey>
    <hubId>1</hubId>
    <deviceType>1004</deviceType>
  </hubActivation>
</request>

Send the file using HTTP POST to the hub activation URL:

curl -X POST -d @req.xml http://esp.peoplepowerco.com/espapi/rest/hubActivation

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

which results in this URL:

https://esp.peoplepowerco.com:8443/deviceio/ml

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


Changelog:
- 2.4.2  23jan12 mwall
* added support for eragy

- 2.4.1  23jan12 mwall
* added support for thingspeak
* added support for pachube

- 2.4.0  17jan2012 mwall
* added new input mode: poll database for data.  with this collection method
    one can run one instance of ecmread to collect data to database and another
    instance of ecmread to periodically read from the database and upload to
    multiple monitoring services.  this reduces risk of missing data when
    online services are slow to respond.  it also makes testing much easier.
* use consistent logging and error reporting through the application
* defined log levels for ERROR, WARN, INFO, and DEBUG
* small changes to database schema.  instead of multiple tables, always use a
    single table, but optionally use an extended schema.
* added support for sqlite

- 2.3.5  14jan2012 mwall
* enable either client or server mode for reading via tcp/ip
* refactor setup/run/read/cleanup for better error handling

- 2.3.4  14jan2012 mwall
* minor refactoring of default parameter names
* improved error/status output
* refactor tcp/ip code to use client pattern rather than server pattern
* apply leading zeros to short serial numbers
* fixed bug in saving of energy data to database

- 2.3.3  28dec2011 mwall
* make the maps optional whenever possible (varies by service)
* group command-line help to make options easier to grok

- 2.3.2  28dec2011 mwall
* improved handling of server responses on upload

- 2.3.1  27dec2011 mwall
* completed testing with enersave
* added compatibility with smart energy groups
* consolidated methods into UploadProcessor class

- 2.3.0  26dec2011 mwall
* use containment not polymorphism to control processing of multiple outputs
* added support for peoplepower

- 2.2.0  - mwall
* consolidate packet reading so socket and serial share the same code
* reject any packet that is not an ecm-1240 packet
* enable multi-ecm support for wattzon
* added support for plotwatt (supercedes plotwatt_v0.1.py)
* added support for EnerSave (supercedes myEnerSave.py)
* added default settings within script to reduce need for command-line options
* use UTC throughout, but display local time
* added option to read parameters from configuration file
* refactor options to make them consistent
* support use of multiple packet processors (e.g. upload to multiple services)
* fixed wattzon usage of urllib to enable multiple, concurrent cloud services

- 2.1.2  22dec2011 mwall
* indicate ecm serial number in checksum mismatch log messages
* use simpler form of extraction for ser_no
* eliminate superfluous use of hex/str/int conversions
* added to packet compilation the DC voltage on aux5
* display reset counter state when printing packets
* respect the reset flag - if a reset has ocurred, skip calculation until
    another packet is added to the buffer

- 2.1.1  20dec2011 mwall
* incorporate ben's packet reading changes from marc's ecmread.py 0.1.5
    for both serial and socket configurations - check for end header bytes.
* validate each packet using checksum.  ignore packet if checksum fails.
* added debug output for diagnosing packet collisions
* cleaned up serial and socket packet reading

- 2.1.0  10dec2011 mwall
* report volts and amps as well as watts
* added option to save watt-hours to database in a separate table
* rename columns from *_ws to *_w (we are recording watts, not watt-seconds)
* to rename columns in a database table, do this in mysql:
    alter table ecm change ch1_ws ch1_w

- 2.0.0  08dec2011 mwall
* support any number of ECM on a single bus when printing or pushing to db.
    this required a change to the database schema, specifically the addition
    of a field to record the ECM serial number.  when uploading to wattson,
    multiple ECM devices are not distinguished.
* display the ECM serial number when printing.
* catch divide-by-zero exceptions when printing.
* bump version to 2.0.0.  the version distributed by marc merlins was listed
    as 1.4.1, but internally as 0.4.1.  the changes to support multiple ECM
    qualify at least as a minor revision, but since they break previous schema
    we'll go with a major revision.

- 0.1.5. 2011/08/25: Ben <ben@brultech.com>
* improved binary packet parsing to better deal with end of packets, and
  remove some corrupted data.
* TODO: actually check the CRC in the binary packet.

- 0.1.4. 2010/06/06: modified screen output code to 
* Show Kwh counters for each input as well as instant W values
* For channel 1 & 2, show positive and negative values.

'''
__author__	= 'Brian Jackson; Kelvin Kakugawa; Marc MERLIN; ben; mwall'

# set skip_upload to print out what would be uploaded but do not actually do
# the upload.
SKIP_UPLOAD = 0

MINUTE	= 60
HOUR	= 60 * MINUTE
DAY	= 24 * HOUR

BUFFER_TIMEFRAME = 5*MINUTE
DEFAULT_UPLOAD_TIMEOUT = 15 # seconds
DEFAULT_UPLOAD_PERIOD = 15*MINUTE

# serial settings
# the com/serial port the ecm is connected to (COM4, /dev/ttyS01, etc)
SERIAL_PORT = "/dev/ttyUSB0"
SERIAL_BAUD = 19200		   # the baud rate we talk to the ecm

# ethernet settings
# the etherbee defaults to pushing data to port 8083
# the wiz110rs defaults to listening on port 5000
IP_HOST = ''      # for client use the hostname/address of the data server
IP_PORT = 8083    # for client use the port of the data server
IP_TIMEOUT = 60
IP_DEFAULT_MODE = 'server'

# database defaults
DB_HOST          = 'localhost'
DB_USER          = ''
DB_PASSWD        = ''
DB_DATABASE      = 'ecm'
DB_TABLE         = 'ecm'
DB_INSERT_PERIOD = MINUTE     # how often to record to database
DB_POLL_INTERVAL = 30        # how often to poll the database, in seconds
DB_FILENAME      = ''
DB_SCHEMA        = 'basic' # basic or extended

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

# eragy defaults
ERAGY_URL           = 'http://d.myeragy.com/energyremote.aspx'
ERAGY_UPLOAD_PERIOD = 15 * MINUTE
ERAGY_TIMEOUT       = 15 # seconds
ERAGY_GATEWAY_ID    = ''
ERAGY_TOKEN         = ''

# smart energy groups defaults
# the map is a comma-delimited list of channel,meter pairs.  for example:
#   311111_ch1,living room,311112_ch1,parlor,311112_aux4,kitchen
SEG_URL           = 'http://api.smartenergygroups.com/api_sites/stream'
SEG_UPLOAD_PERIOD = MINUTE
SEG_TIMEOUT       = 15 # seconds
SEG_TOKEN         = ''
SEG_MAP           = ''

# thingspeak defaults
#   Uploads are limited to no more than every 15 seconds per channel.
TS_URL           = 'http://api.thingspeak.com/update'
TS_UPLOAD_PERIOD = MINUTE
TS_TIMEOUT       = 15 # seconds
TS_TOKENS        = ''
TS_FIELDS        = ''

# pachube defaults
PBE_URL           = 'http://api.pachube.com/v2/feeds'
PBE_UPLOAD_PERIOD = MINUTE
PBE_TIMEOUT       = 15 # seconds
PBE_TOKEN         = ''
PBE_FEED          = ''


import base64
import bisect
import new
import optparse
import socket
import sys
import time
import traceback
import urllib
import urllib2

import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning) # MySQLdb in 2.6.*

# External (Optional) Dependencies
try:
    import serial
except Exception, e:
    serial = None

try:
    import MySQLdb
except Exception, e:
    MySQLdb = None

try:
    from sqlite3 import dbapi2 as sqlite
except Exception, e:
    sqlite = None

try:
    import cjson as json
    # XXX: maintain compatibility w/ json module
    setattr(json, 'dumps', json.encode)
    setattr(json, 'loads', json.decode)
except Exception, e:
    try:
        import simplejson as json
    except Exception, e:
        import json

try:
    import ConfigParser
except Exception, e:
    ConfigParser = None


# settings for ECM-1240 packets
START_HEADER0	  = 254
START_HEADER1	  = 255
ECM1240_PACKET_ID = 3
END_HEADER0       = 255
END_HEADER1       = 254
DATA_BYTES_LENGTH = 59             # does not include the start/end headers
SEC_COUNTER_MAX   = 16777216
ECM1240_UNIT_ID   = 3
ECM1240_CHANNELS  = ['ch1', 'ch2', 'aux1', 'aux2', 'aux3', 'aux4', 'aux5']

class CounterResetError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

# logging and error reporting
#
# note that setting the log level to debug will affect the application
# behavior, especially when sampling the serial line, as it changes the
# timing of read operations.
LOG_ERROR = 0
LOG_WARN  = 1
LOG_INFO  = 2
LOG_DEBUG = 3
LOGLEVEL  = 3

def dbgmsg(msg):
    if LOGLEVEL >= LOG_DEBUG:
        logmsg(msg)

def infmsg(msg):
    if LOGLEVEL >= LOG_INFO:
        logmsg(msg)

def wrnmsg(msg):
    if LOGLEVEL >= LOG_WARN:
        logmsg(msg)

def errmsg(msg):
    if LOGLEVEL >= LOG_ERROR:
        logmsg(msg)

def logmsg(msg):
    ts = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())
    print "%s %s" % (ts, msg)

# Helper Functions

def getgmtime():
    return int(time.time())

def cleanvalue(s):
    '''ensure that values read from configuration file are sane'''
    s = s.replace('\n', '')    # we never want newlines
    s = s.replace('\r', '')    # or carriage returns
    if s.lower() == 'false':
        s = False
    elif s.lower() == 'true':
        s = True
    return s

def pairs2dict(s):
    '''convert comma-delimited name,value pairs to a dictionary'''
    items = s.split(',')
    m = {}
    for k, v in zip(items[::2], items[1::2]):
        m[k] = v
    return m

def getresetcounter(byte):
    '''extract the reset counter from a byte'''
    return byte & 0b00000111      # same as 0x07

def getserial(packet):
    '''extract the ECM serial number from a compiled packet'''
    return "%d%05d" % (packet['unit_id'], packet['ser_no'])

def getserialraw(packet):
    '''extract the ECM serial number from a raw packet'''
    sn1 = ord(packet[26:27])
    sn2 = ord(packet[27:28]) * 256
    id1 = ord(packet[29:30])
    return "%d%05d" % (id1, sn1+sn2)

def obfuscate_serial(sn):
    '''obfuscate a brultech serial number - expose the last 3 digits of 6'''
    n = len(sn)
    return 'XXX%s' % sn[n-3:n]

def compare_packet_times(a, b):
    return cmp(a['time_created'], b['time_created'])

def calculate_checksum(packet):
    '''calculate the packet checksum'''
    checksum = START_HEADER0
    checksum += START_HEADER1
    checksum += ECM1240_PACKET_ID
    checksum += sum([ord(c) for c in packet])
    checksum += END_HEADER0
    checksum += END_HEADER1
    return checksum & 0xff

def calculate(now, prev):
    '''calc average watts/s between packets'''

    # if reset counter has changed since last packet, skip the calculation
    c0 = getresetcounter(prev['flag'])
    c1 = getresetcounter(now['flag'])
    if c1 != c0:
        raise CounterResetError("old: %d new: %d" % (c0, c1))

    if now['secs'] < prev['secs']:
        now['secs'] += SEC_COUNTER_MAX # handle seconds counter overflow
    secs_delta = float(now['secs'] - prev['secs'])

    ret = now

    # CH1/2 Watts
    ret['ch1_watts'] = (ret['ch1_aws'] - prev['ch1_aws']) / secs_delta
    ret['ch2_watts'] = (ret['ch2_aws'] - prev['ch2_aws']) / secs_delta

    ret['ch1_positive_watts'] = (ret['ch1_pws'] - prev['ch1_pws']) / secs_delta
    ret['ch2_positive_watts'] = (ret['ch2_pws'] - prev['ch2_pws']) / secs_delta

    ret['ch1_negative_watts'] = ret['ch1_watts'] - ret['ch1_positive_watts']
    ret['ch2_negative_watts'] = ret['ch2_watts'] - ret['ch2_positive_watts']

    # All Watts increase no matter which way the current is going
    # Polar Watts only increase if the current is positive
    # Every Polar Watt does register as an All Watt too.
    # math comes to: Watts = 2x Polar Watts - All Watts
    ret['ch1_pwh'] = ret['ch1_pws'] / 3600000.0
    ret['ch2_pwh'] = ret['ch2_pws'] / 3600000.0
    ret['ch1_nwh'] = (ret['ch1_aws'] - ret['ch1_pws']) / 3600000.0
    ret['ch2_nwh'] = (ret['ch2_aws'] - ret['ch2_pws']) / 3600000.0
    ret['ch1_wh']  = ret['ch1_pwh'] - ret['ch1_nwh']
    ret['ch2_wh']  = ret['ch2_pwh'] - ret['ch2_nwh']

    ret['aux1_wh'] = ret['aux1_ws'] / 3600000.0
    ret['aux2_wh'] = ret['aux2_ws'] / 3600000.0
    ret['aux3_wh'] = ret['aux3_ws'] / 3600000.0
    ret['aux4_wh'] = ret['aux4_ws'] / 3600000.0
    ret['aux5_wh'] = ret['aux5_ws'] / 3600000.0

    # Polar Watts' instant value's only purpose seems to help find out if
    # main watts are positive or negative. Polar Watts only goes up if the
    # sign is positive. If they are null, tha means the sign is negative.
    if (ret['ch1_positive_watts'] == 0):
        ret['ch1_watts'] *= -1 
    if (ret['ch2_positive_watts'] == 0):
        ret['ch2_watts'] *= -1 

    # AUX1-5 Watts
    ret['aux1_watts'] = (ret['aux1_ws'] - prev['aux1_ws']) / secs_delta
    ret['aux2_watts'] = (ret['aux2_ws'] - prev['aux2_ws']) / secs_delta
    ret['aux3_watts'] = (ret['aux3_ws'] - prev['aux3_ws']) / secs_delta
    ret['aux4_watts'] = (ret['aux4_ws'] - prev['aux4_ws']) / secs_delta
    ret['aux5_watts'] = (ret['aux5_ws'] - prev['aux5_ws']) / secs_delta

    return ret


class Buffer:
  def __init__(self):
    self.buf = ""

  def __len__(self):
    return len(self.buf)

  def get_bytes(self, start, end):
    return self.buf[start:start+end]

  def get_byte(self, num):
    if len(self.buf) < num + 1:
        raise Exception("Unepected request %d - %d" % (num, len(self.buf)))
    byte = ord(self.buf[num])
    return byte

  def remove(self, num):
    ret = self.buf[0:num]
    self.buf = self.buf[num:]
    return ret

  def append(self, buf):
    self.buf += buf

# Data Collector classes

class BaseDataCollector(object):
    def __init__(self, packet_processor):
        self.packet_processor = packet_processor
        dbgmsg('using %d processors:' % len(self.packet_processor))
        for p in self.packet_processor:
            dbgmsg('  %s' % p.__class__.__name__)

    def setup(self):
        pass

    def cleanup(self):
        pass

    # The read method collects data then passes it to each of the processors.
    def read(self):
        pass

    # Loop forever, break only for keyboard interrupts.
    def run(self):
        try:
            self.setup()
            for p in self.packet_processor:
                dbgmsg('setup %s' % p.__class__.__name__)
                p.setup()

            while True:
                try:
                    self.read()
                except KeyboardInterrupt, e:
                    raise e
                except Exception, e:
                    wrnmsg(e)

        except KeyboardInterrupt:
            sys.exit(0)
        except Exception, e:
            if LOGLEVEL >= LOG_DEBUG:
                traceback.print_exc()
            else:
                errmsg(e)
            sys.exit(1)

        finally:
            for p in self.packet_processor:
                dbgmsg('cleanup %s' % p.__class__.__name__)
                p.cleanup()
            self.cleanup()


class BufferedDataCollector(BaseDataCollector):
    def __init__(self, packet_processor):
        super(BufferedDataCollector, self).__init__(packet_processor)
        self.packet_buffer = CompoundBuffer(BUFFER_TIMEFRAME)
        dbgmsg('buffer size is %d' % BUFFER_TIMEFRAME)
        self.buf = Buffer()

    def _convert(self, bytes):
        return reduce(lambda x,y:x+y[0] * (256**y[1]), zip(bytes,xrange(len(bytes))),0)

    def _compile(self, packet):
        now = {}

        # Voltage Data (2 bytes)
        now['volts'] = 0.1 * self._convert(packet[1::-1])

        # CH1-2 Absolute Watt-Second Counter (5 bytes each)
        now['ch1_aws'] = self._convert(packet[2:7])
        now['ch2_aws'] = self._convert(packet[7:12])

        # CH1-2 Polarized Watt-Second Counter (5 bytes each)
        now['ch1_pws'] = self._convert(packet[12:17])
        now['ch2_pws'] = self._convert(packet[17:22])

        # Reserved (4 bytes)

        # Device Serial Number (2 bytes)
        now['ser_no'] = self._convert(packet[26:28])

        # Reset and Polarity Information (1 byte)
        now['flag'] = self._convert(packet[28:29])

        # Device Information (1 byte)
        now['unit_id'] = self._convert(packet[29:30])

        # CH1-2 Current (2 bytes each)
        now['ch1_amps'] = 0.01 * self._convert(packet[30:32])
        now['ch2_amps'] = 0.01 * self._convert(packet[32:34])

        # Seconds (3 bytes)
        now['secs'] = self._convert(packet[34:37])

        # AUX1-5 Watt-Second Counter (4 bytes each)
        now['aux1_ws'] = self._convert(packet[37:41])
        now['aux2_ws'] = self._convert(packet[41:45])
        now['aux3_ws'] = self._convert(packet[45:49])
        now['aux4_ws'] = self._convert(packet[49:53])
        now['aux5_ws'] = self._convert(packet[53:57])

        # DC voltage on AUX5 (2 bytes)
        now['aux5_volts'] = self._convert(packet[57:59])

        return now

    def read(self):
      while True:
        self.read_data()

    # Called by derived classes when data has been read
    def data_read(self, new_data):
      self.buf.append(new_data)
      data = self.buf
      while len(data) >= 65:
        header = [START_HEADER0, START_HEADER1, ECM1240_PACKET_ID]
        valid = True
        for i in range(len(header)):
          byte = data.get_byte(i)
          if byte != header[i]:
              dbgmsg("expected START_HEADER%d %s, got %s" %
                     (i, hex(header[i]), hex(byte)))
              data.remove(i + 1)
              valid = False
              break
        if not valid:
          continue

        packet = data.get_bytes(len(header), DATA_BYTES_LENGTH)

        footer = [END_HEADER0, END_HEADER1]
        for i in range(len(footer)):
          byte = data.get_byte(len(header) + DATA_BYTES_LENGTH + i)
          if byte != footer[i]:
              dbgmsg("expected END_HEADER%d %s, got %s" %
                     (i, hex(footer[i]), hex(byte)))
              data.remove(len(header) + DATA_BYTES_LENGTH + i + 1)
              valid = False
              break
        if not valid:
          continue

        # we only handle ecm-1240 devices
        uid = ord(packet[29:30])
        if uid != ECM1240_UNIT_ID:
            infmsg("unrecognized unit id: expected %s, got %s" %
                   (hex(ECM1240_UNIT_ID), hex(uid)))
            data.remove(59 + 3 + 2)
            continue

        # if the checksum is incorrect, ignore the packet
        checksum = calculate_checksum(packet)
        print "Checking byte %d" % (len(header) + DATA_BYTES_LENGTH + len(footer))
        byte = data.get_byte(64)
        full_packet = data.remove(65)
        if byte != checksum:
            infmsg("bad checksum for %s: expected %s, got %s" %
                   (getserialraw(packet), hex(checksum), hex(byte)))
            continue

        packet = [ord(c) for c in packet]
        packet = self._compile(packet)
        print "Processing packet"
        packet['time_created'] = getgmtime()
        self.packet_buffer.insert(packet['time_created'], packet)
        self.process(packet)

    # process a compiled packet
    def process(self, packet):
        for p in self.packet_processor:
            try:
                dbgmsg('processing with %s' % p.__class__.__name__)
                p.process_compiled(packet, self.packet_buffer)
            except Exception, e:
                if not p.handle(e):
                    wrnmsg('Exception in %s: %s' % (p.__class__.__name__, e))
                    if LOGLEVEL >= LOG_DEBUG:
                        traceback.print_exc()


class SerialCollector(BufferedDataCollector):
    def __init__(self, packet_processor, port=SERIAL_PORT, rate=SERIAL_BAUD):
        super(SerialCollector, self).__init__(packet_processor)

        if not serial:
            print 'Error: serial module could not be imported.'
            sys.exit(1)

        self._port	= port
        self._baudrate = int(rate)
        self.conn = None

        infmsg('serial port: %s' % self._port)
        infmsg('baud rate: %d' % self._baudrate)

    def read_data(self):
        self.data_read(self.conn.read(8096))

    def setup(self):
        self.conn = serial.Serial(self._port, self._baudrate, timeout=0)
        self.conn.open()

    def cleanup(self):
        if self.conn:
            self.conn.close()
            self.conn = None


class SocketServerCollector(BufferedDataCollector):
    def __init__(self, packet_processor, host=IP_HOST, port=IP_PORT):
        super(SocketServerCollector, self).__init__(packet_processor)
        socket.setdefaulttimeout(IP_TIMEOUT)
        self._host = host
        self._port = int(port)
        self.sock = None
        self.conn = None
        infmsg('host: %s' % self._host)
        infmsg('port: %d' % self._port)

    def read_data(self):
        self.data_read(self.conn.recv(8096))

    def read(self):
        try:
            self.conn, addr = self.sock.accept()
            super(SocketServerCollector, self).read();
        finally:
            if self.conn:
                self.conn.shutdown(socket.SHUT_RD)
                self.conn.close()
                self.conn = None

    def setup(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except: # REUSEPORT may not be supported on all systems  
            pass
        self.sock.bind((self._host, self._port))
        self.sock.listen(1)

    def cleanup(self):
        if self.sock:
            self.sock.close()
            self.sock = None


class SocketClientCollector(BufferedDataCollector):
    def __init__(self, packet_processor, host=IP_HOST, port=IP_PORT):
        super(SocketClientCollector, self).__init__(packet_processor)
        socket.setdefaulttimeout(IP_TIMEOUT)
        self._host = host
        self._port = int(port)
        self.sock = None
        infmsg('host: %s' % self._host)
        infmsg('port: %d' % self._port)

    def read_data(self, count):
        self.data_read(sock.recv(count))

    def read(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self._host, self._port))
            super(SocketClientCollector, self).read();
        finally:
            if self.sock:
                self.sock.close()
                self.sock = None


class DatabaseCollector(BaseDataCollector):
    def __init__(self, packet_processor, host, database, username, password):
        super(DatabaseCollector, self).__init__(packet_processor)

        self._host = host
        self._database = database
        self._username = username
        self._password = password
        self._table    = DB_TABLE
        self._poll_interval = DB_POLL_INTERVAL
        self._conn = None
        self._lastread = getgmtime() - self._poll_interval

        infmsg('DB: host: %s' % self._host)
        infmsg('DB: username: %s' % self._username)
        infmsg('DB: database: %s' % self._database)
        infmsg('DB: polling interval: %d seconds' % self._poll_interval)

    def setup(self):
        self._conn = MySQLdb.connect(host=self._host,
                                     user=self._username,
                                     passwd=self._password,
                                     db=self._database)

    def cleanup(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def read(self):
        cursor = self._conn.cursor()
        cursor.execute('select * from ' + self._table + ' where time_created > ' + str(self._lastread))
        # FIXME: limit number of items that we will accept from db query
        rows = cursor.fetchall()
        dbgmsg('DB: query returned %d rows' % len(rows))
        self._lastread = getgmtime()
        packets = {}
        for row in rows:
            sn = str(row[2]) # FIXME: get this by name, not index
            if not sn in packets:
                packets[sn] = []
            packets[sn].append(self.row2packet(row))
        cursor.close()
        for sn in packets:
            self.process(sn, packets[sn])
        time.sleep(self._poll_interval)

    # FIXME: sort out the processing timing.  if one or more processors does
    # not process packets, we should buffer the packets for the processor to
    # handle the next time.  or some other logic so that a processor with long
    # period does not end up missing all of the in-between packets.

    # process a list of calculated packets
    def process(self, ecm_serial, packets):
        packets.sort(compare_packet_times)
        for p in self.packet_processor:
            try:
                dbgmsg('processing with %s' % p.__class__.__name__)
                p.process_calculated(ecm_serial, packets)
            except Exception, e:
                if not p.handle(e):
                    wrnmsg('Exception in %s: %s' % (p.__class__.__name__, e))
                    if LOGLEVEL >= LOG_DEBUG:
                        traceback.print_exc()

    # FIXME: infer the schema automatically from the database columns
    def row2packet(self, row):
        sn = str(row[2])
        p = {}
        p['flag'] = 0 # fake it
        p['unit_id'] = int(sn[0])
        p['ser_no'] = int(sn[1:])
        p['time_created'] = long(row[1])
        p['volts'] = float(row[3])
        p['ch1_amps'] = float(row[4])
        p['ch2_amps'] = float(row[5])
        p['ch1_watts'] = int(row[6])
        p['ch2_watts'] = int(row[7])
        p['aux1_watts'] = int(row[8])
        p['aux2_watts'] = int(row[9])
        p['aux3_watts'] = int(row[10])
        p['aux4_watts'] = int(row[11])
        p['aux5_watts'] = int(row[12])
        if DB_SCHEMA == 'extended':
            p['ch1_wh'] = int(row[13])
            p['ch2_wh'] = int(row[14])
            p['aux1_wh'] = int(row[15])
            p['aux2_wh'] = int(row[16])
            p['aux3_wh'] = int(row[17])
            p['aux4_wh'] = int(row[18])
            p['aux5_wh'] = int(row[19])
            p['ch1_positive_watts'] = int(row[20])
            p['ch1_negative_watts'] = int(row[21])
            p['ch2_positive_watts'] = int(row[22])
            p['ch2_negative_watts'] = int(row[23])
            p['ch1_pwh'] = int(row[24])
            p['ch1_nwh'] = int(row[25])
            p['ch2_pwh'] = int(row[26])
            p['ch2_nwh'] = int(row[27])
        else:
            p['ch1_wh'] = 0
            p['ch2_wh'] = 0
            p['aux1_wh'] = 0
            p['aux2_wh'] = 0
            p['aux3_wh'] = 0
            p['aux4_wh'] = 0
            p['aux5_wh'] = 0
            p['ch1_positive_watts'] = 0
            p['ch1_negative_watts'] = 0
            p['ch2_positive_watts'] = 0
            p['ch2_negative_watts'] = 0
            p['ch1_pwh'] = 0
            p['ch1_nwh'] = 0
            p['ch2_pwh'] = 0
            p['ch2_nwh'] = 0
        return p

# Buffer Classes

class MovingBuffer(object):
    '''Maintain fixed-size buffer of data over time'''
    def __init__(self, max_timeframe=DAY):
        self.time_points	= []
        self.max_timeframe	= max_timeframe

    def insert(self, timestamp, time_dict):
        bisect.insort(self.time_points, (timestamp, time_dict))
        now = getgmtime()
        cull_index = bisect.bisect(self.time_points, (now-self.max_timeframe, {}))
        del(self.time_points[:cull_index])

    def data_over(self, time_delta):
        now = getgmtime()
        delta_index = bisect.bisect(self.time_points, (now-time_delta, {}))
        return self.time_points[delta_index:]

    def delta_over(self, time_delta):
        now = getgmtime()
        delta_index = bisect.bisect(self.time_points, (now-time_delta, {}))
        offset = self.time_points[delta_index][1]
        current = self.time_points[-1][1]
        return calculate(current, offset)

    def size(self):
        return len(self.time_points)

class CompoundBuffer(object):
    '''Variable number of moving buffers, each associated with an ID'''
    def __init__(self, max_timeframe=DAY):
        self.max_timeframe = max_timeframe
        self.buffers = {}

    def insert(self, timestamp, time_dict):
        ecm_serial = getserial(time_dict)
        return self.getbuffer(ecm_serial).insert(timestamp, time_dict)

    def data_over(self, ecm_serial, time_delta):
        return self.getbuffer(ecm_serial).data_over(time_delta)

    def delta_over(self, ecm_serial, time_delta):
        return self.getbuffer(ecm_serial).delta_over(time_delta)

    def size(self, ecm_serial):
        return self.getbuffer(ecm_serial).size()

    def getbuffer(self, ecm_serial):
        if not ecm_serial in self.buffers:
            self.buffers[ecm_serial] = MovingBuffer(self.max_timeframe)
        return self.buffers[ecm_serial]

# Packet Processor Classes

class BaseProcessor(object):
    def __init__(self, *args, **kwargs):
        pass

    def setup(self):
        pass

    def process_calculated(self, ecm_serial, packets):
        pass

    def process_compiled(self, packet, packet_buffer):
        pass
	
    def handle(self, exception):
        return False

    def cleanup(self):
        pass


class PrintProcessor(BaseProcessor):
    def __init__(self, *args, **kwargs):
        super(PrintProcessor, self).__init__(*args, **kwargs)
        self.prev_packet = {}

    def process_compiled(self, packet, packet_buffer):
        sn = getserial(packet)
        if sn in self.prev_packet:
            try:
                p = calculate(packet, self.prev_packet[sn])
            except ZeroDivisionError, zde:
                infmsg("not enough data in buffer for %s" % sn)
                return
            except CounterResetError, cre:
                wrnmsg("counter reset for %s: %s" % (sn, cre.msg))
                return
            self.process_calculated(sn, [p])
        self.prev_packet[sn] = packet

    def process_calculated(self, ecm_serial, packets):
        for p in packets:
            ts = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(p['time_created']))

            # start with newline in case previous run was stopped in the middle
            # of a line.  this ensures that the new output is not attached to
            # some old incompletely written line.
            print
            print ts+": ECM: %s" % ecm_serial
            print ts+": Counter: %d" % getresetcounter(p['flag'])
            print ts+": Volts:              %9.2fV" % p['volts']
            print ts+": Ch1 Amps:           %9.2fA" % p['ch1_amps']
            print ts+": Ch2 Amps:           %9.2fA" % p['ch2_amps']
            print ts+": Ch1 Watts:          % 13.6fKWh (% 5dW)" % (p['ch1_wh'] , p['ch1_watts'])
            print ts+": Ch1 Positive Watts: % 13.6fKWh (% 5dW)" % (p['ch1_pwh'], p['ch1_positive_watts'])
            print ts+": Ch1 Negative Watts: % 13.6fKWh (% 5dW)" % (p['ch1_nwh'], p['ch1_negative_watts'])
            print ts+": Ch2 Watts:          % 13.6fKWh (% 5dW)" % (p['ch2_wh'] , p['ch2_watts'])
            print ts+": Ch2 Positive Watts: % 13.6fKWh (% 5dW)" % (p['ch2_pwh'], p['ch2_positive_watts'])
            print ts+": Ch2 Negative Watts: % 13.6fKWh (% 5dW)" % (p['ch2_nwh'], p['ch2_negative_watts'])
            print ts+": Aux1 Watts:         % 13.6fKWh (% 5dW)" % (p['aux1_wh'], p['aux1_watts'])
            print ts+": Aux2 Watts:         % 13.6fKWh (% 5dW)" % (p['aux2_wh'], p['aux2_watts'])
            print ts+": Aux3 Watts:         % 13.6fKWh (% 5dW)" % (p['aux3_wh'], p['aux3_watts'])
            print ts+": Aux4 Watts:         % 13.6fKWh (% 5dW)" % (p['aux4_wh'], p['aux4_watts'])
            print ts+": Aux5 Watts:         % 13.6fKWh (% 5dW)" % (p['aux5_wh'], p['aux5_watts'])


class DatabaseProcessor(BaseProcessor):
    def __init__(self, *args, **kwargs):
        super(DatabaseProcessor, self).__init__(*args, **kwargs)
        self.db_table = DB_TABLE
        self.conn = None

    def setup(self):
        self.insert_period = DB_INSERT_PERIOD
        self.last_insert   = {}

    def process_compiled(self, packet, packet_buffer):
        sn = getserial(packet)
        now = getgmtime()
        if sn in self.last_insert and now < (self.last_insert[sn] + self.insert_period):
            return

        try:
            delta = packet_buffer.delta_over(sn, self.insert_period)
        except ZeroDivisionError, zde:
            infmsg("DB: not enough data in buffer for %s" % sn)
            return
        except CounterResetError, cre:
            wrnmsg('DB: counter reset for %s: %s' % (sn, cre.msg))
            return
        self.process_calculated(sn, [delta])
        self.last_insert[sn] = now

    def process_calculated(self, ecm_serial, packets):
        for p in packets:
            if DB_SCHEMA == 'extended':
                sql = 'INSERT INTO %s (\
 time_created, ecm_serial, volts, ch1_a, ch2_a,\
 ch1_w, ch2_w, aux1_w, aux2_w, aux3_w, aux4_w, aux5_w,\
 ch1_wh, ch2_wh, aux1_wh, aux2_wh, aux3_wh, aux4_wh, aux5_wh,\
 ch1_pw, ch1_nw, ch2_pw, ch2_nw, ch1_pwh, ch1_nwh, ch2_pwh, ch2_nwh\
) VALUES (\
 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
 %s, %s, %s, %s, %s, %s, %s)' % (
  self.db_table,
  str(p['time_created']), str(ecm_serial),
  str(p['volts']), str(p['ch1_amps']), str(p['ch2_amps']),
  str(p['ch1_watts']), str(p['ch2_watts']),
  str(p['aux1_watts']), str(p['aux2_watts']), str(p['aux3_watts']),
  str(p['aux4_watts']), str(p['aux5_watts']),
  str(p['ch1_wh']), str(p['ch2_wh']),
  str(p['aux1_wh']), str(p['aux2_wh']), str(p['aux3_wh']),
  str(p['aux4_wh']), str(p['aux5_wh']),
  str(p['ch1_positive_watts']), str(p['ch1_negative_watts']),
  str(p['ch2_positive_watts']), str(p['ch2_negative_watts']),
  str(p['ch1_pwh']), str(p['ch1_nwh']), str(p['ch2_pwh']), str(p['ch2_nwh'])
  )
            else:
                sql = 'INSERT INTO %s (\
 time_created, ecm_serial, volts, ch1_a, ch2_a,\
 ch1_w, ch2_w, aux1_w, aux2_w, aux3_w, aux4_w, aux5_w\
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' % (
  self.db_table,
  str(p['time_created']), str(ecm_serial),
  str(p['volts']), str(p['ch1_amps']), str(p['ch2_amps']),
  str(p['ch1_watts']), str(p['ch2_watts']),
  str(p['aux1_watts']), str(p['aux2_watts']), str(p['aux3_watts']),
  str(p['aux4_watts']), str(p['aux5_watts'])
  )
            dbgmsg('DB: query: %s' % sql)
            cursor = self.conn.cursor()
            cursor.execute(sql)
            cursor.close()
            infmsg('DB: inserted @%s: sn: %s, v: %s, ch1a: %s, ch2a: %s, ch1w: %s, ch2w: %s, aux1w: %s, aux2w: %s, aux3w: %s, aux4w: %s, aux5w: %s' % (
                    p['time_created'], ecm_serial,
                    p['volts'], p['ch1_amps'], p['ch2_amps'],
                    p['ch1_watts'], p['ch2_watts'],
                    p['aux1_watts'], p['aux2_watts'], p['aux3_watts'],
                    p['aux4_watts'], p['aux5_watts'],
                    ))
        self.conn.commit()


class MySqlProcessor(DatabaseProcessor):
    def __init__(self, *args, **kwargs):
        super(MySqlProcessor, self).__init__(*args, **kwargs)

        if not MySQLdb:
            print 'DB Error: MySQLdb module could not be imported.'
            sys.exit(1)

        self.db_host     = kwargs.get('db_host')	 or DB_HOST
        self.db_user     = kwargs.get('db_user')	 or DB_USER
        self.db_passwd   = kwargs.get('db_passwd')   or DB_PASSWD
        self.db_database = kwargs.get('db_database') or DB_DATABASE
        self.db_table    = self.db_database + '.' + self.db_table

        infmsg('DB: host: %s' % self.db_host)
        infmsg('DB: username: %s' % self.db_user)
        infmsg('DB: database: %s' % self.db_database)

    def setup(self):
        super(MySqlProcessor, self).setup()
        self.conn = MySQLdb.connect(host=self.db_host,
                                    user=self.db_user,
                                    passwd=self.db_passwd,
                                    db=self.db_database)

    def handle(self, e):
        if type(e) == MySQLdb.Error:
            errmsg('MySQL Error: [#%d] %s' % (e.args[0], e.args[1]))
            return True
        return super(MySqlProcessor, self).handle(e)

    def cleanup(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()


class SqliteProcessor(DatabaseProcessor):
    def __init__(self, *args, **kwargs):
        super(SqliteProcessor, self).__init__(*args, **kwargs)

        if not sqlite:
            print 'DB Error: sqlite3 module could not be imported.'
            sys.exit(1)

        self.db_file = kwargs.get('db_file') or DB_FILENAME
        if not (self.db_file):
            print 'DB Error: no database file specified'
            sys.exit(1)

        infmsg('DB: file: %s' % self.db_file)

    def setup(self):
        super(SqliteProcessor, self).setup()
        self.conn = sqlite.connect(self.db_file)

    def cleanup(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()


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
        req.add_header("User-Agent", "ecmread/%s" % __version__)
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


class WattzOnProcessor(UploadProcessor):
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
                        'unit':	'W',
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


class PlotWattProcessor(UploadProcessor):
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


class EnerSaveProcessor(UploadProcessor):
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


class PeoplePowerProcessor(UploadProcessor):
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


class EragyProcessor(UploadProcessor):
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

    def process_calculated(self, ecm_serial, packets):
        osn = obfuscate_serial(ecm_serial)
        s = []
        for p in packets:
            for idx,c in enumerate(ECM1240_CHANNELS):
                key = osn + '_' + c
                s.append('<MTU ID="%s"><cumulative timestamp="%s" watts="%d"/></MTU>' % (key,p['time_created'],p[c+'_watts']))
        if len(s):
            s.insert(0, '<ted5000 GWID="%s" auth="%s">' %
                     (self.gateway_id, self.token))
            s.append('</ted5000>')
            result = self._urlopen(ecm_serial, self.url, ''.join(s))
            resp = result.read()
            if not resp == '<xml>0</xml>':
                wrnmsg('EG: upload failed for %s: %s' % (ecm_serial, resp))
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


class SmartEnergyGroupsProcessor(UploadProcessor):
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


class ThingSpeakProcessor(UploadProcessor):
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


class PachubeProcessor(UploadProcessor):
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


if __name__ == '__main__':
    parser = optparse.OptionParser(version=__version__)

    parser.add_option('-c', '--config-file', dest='configfile', help='read configuration from FILE', metavar='FILE')
    parser.add_option('-q', '--quiet', action='store_true', dest='quiet', default=False, help='quiet output')
    parser.add_option('-v', '--verbose', action='store_false', dest='quiet', default=False, help='verbose output')
    parser.add_option('--debug', action='store_true', default=False, help='debug output')

    parser.add_option('-p', '--print', action='store_true', dest='print_out', default=False, help='print data to screen')

    group = optparse.OptionGroup(parser, 'data source options')
    group.add_option('--serial', action='store_true', dest='serial_read', default=False, help='read from serial port')
    group.add_option('--serialport', dest='serial_port', help='serial port')
    group.add_option('--baudrate', dest='serial_baud', help='serial baud rate')

    group.add_option('--ip', action='store_true', dest='ip_read', default=False, help='read from TCP/IP source such as EtherBee')
    group.add_option('--ip-host', help='ip host')
    group.add_option('--ip-port', help='ip port')
    group.add_option('--ip-mode', help='act as client or server')
    parser.add_option_group(group)

    group.add_option('--db', action='store_true', dest='db_read', default=False, help='read from database')
    group.add_option('--db-read-host', help='source database host')
    group.add_option('--db-read-user', help='source database user')
    group.add_option('--db-read-passwd', help='source database password')
    group.add_option('--db-read-database', help='source database name')

    group = optparse.OptionGroup(parser, 'database options')
    group.add_option('-d', '--database', action='store_true', dest='db_out', default=False, help='write data to mysql database')
    group.add_option('--db-host', help='database host')
    group.add_option('--db-user', help='database user')
    group.add_option('--db-passwd', help='database password')
    group.add_option('--db-database', help='database name')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'sqlite options')
    group.add_option('--sqlite', action='store_true', dest='sqlite_out', default=False, help='write data to sqlite database')
    group.add_option('--db-file', help='database filename')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'WattzOn options')
    group.add_option('--wattzon', action='store_true', dest='wattzon_out', default=False, help='upload data using WattzOn API')
    group.add_option('--wo-user', help='username')
    group.add_option('--wo-pass', help='password')
    group.add_option('--wo-api-key', help='API key')
    group.add_option('--wo-map', help='channel-to-meter mapping')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'PlotWatt options')
    group.add_option('--plotwatt', action='store_true', dest='plotwatt_out', default=False, help='upload data using PlotWatt API')
    group.add_option('--pw-house-id', help='house ID')
    group.add_option('--pw-api-key', help='API key')
    group.add_option('--pw-map', help='channel-to-meter mapping')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'EnerSave options')
    group.add_option('--enersave', action='store_true', dest='enersave_out', default=False, help='upload data using EnerSave API')
    group.add_option('--es-token', help='token')
    group.add_option('--es-url', help='URL')
    group.add_option('--es-map', help='channel-to-device mapping')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'PeoplePower options')
    group.add_option('--peoplepower', action='store_true', dest='peoplepower_out', default=False, help='upload data using PeoplePower API')
    group.add_option('--pp-token', help='auth token')
    group.add_option('--pp-hub-id', help='hub ID')
    group.add_option('--pp-url', help='URL')
    group.add_option('--pp-map', help='channel-to-device mapping')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'Eragy options')
    group.add_option('--eragy', action='store_true', dest='eragy_out', default=False, help='upload data using Eragy API')
    group.add_option('--eg-gateway-id', help='gateway id')
    group.add_option('--eg-token', help='token')
    group.add_option('--eg-url', help='URL')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'Smart Energy Groups options')
    group.add_option('--smartenergygroups', action='store_true', dest='smartenergygroups_out', default=False, help='upload data using SmartEnergyGroups API')
    group.add_option('--seg-token', help='token')
    group.add_option('--seg-url', help='URL')
    group.add_option('--seg-map', help='channel-to-device mapping')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'ThingSpeak options')
    group.add_option('--thingspeak', action='store_true', dest='thingspeak_out', default=False, help='upload data using ThingSpeak API')
    group.add_option('--ts-url', help='URL')
    group.add_option('--ts-tokens', help='ECM-to-ID/token mapping')
    group.add_option('--ts-fields', help='channel-to-field mapping')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'Pachube options')
    group.add_option('--pachube', action='store_true', dest='pachube_out', default=False, help='upload data using Pachube API')
    group.add_option('--pbe-url', help='URL')
    group.add_option('--pbe-token', help='token')
    group.add_option('--pbe-feed', help='feed')
    parser.add_option_group(group)

    (options, args) = parser.parse_args()

    # if there is a configration file, read the parameters from file and set
    # values on the options object.
    if options.configfile:
        if not ConfigParser:
            print 'ConfigParser not loaded, cannot parse config file'
            sys.exit(1)
        config = ConfigParser.ConfigParser()
        config.read(options.configfile)
        for section in config.sections(): # section names do not matter
            for name,value in config.items(section):
                if not getattr(options, name):
                    setattr(options, name, cleanvalue(value))

    if options.quiet:
        LOGLEVEL = LOG_ERROR
    if options.debug:
        LOGLEVEL = LOG_DEBUG

    # Packet Processor Setup
    if not (options.print_out or options.db_out or options.sqlite_out or options.wattzon_out or options.plotwatt_out or options.enersave_out or options.peoplepower_out or options.eragy_out or options.smartenergygroups_out or options.thingspeak_out or options.pachube_out):
        print 'Please specify one or more processing options (or \'-h\' for help):'
        print '  --print              print to screen'
        print '  --database           write to mysql database'
        print '  --sqlite             write to sqlite database'
        print '  --wattzon            upload to WattzOn'
        print '  --plotwatt           upload to PlotWatt'
        print '  --enersave           upload to EnerSave'
        print '  --peoplepower        upload to PeoplePower'
        print '  --eragy              upload to Eragy'
        print '  --smartenergygroups  upload to SmartEnergyGroups'
        print '  --thingspeak         upload to ThingSpeak'
        print '  --pachube            upload to Pachube'
        sys.exit(1)

    procs = []

    if options.print_out:
        procs.append(PrintProcessor(args, **{ }))
    if options.db_out:
        procs.append(MySqlProcessor(args, **{
                    'db_host':      options.db_host,
                    'db_user':      options.db_user,
                    'db_passwd':    options.db_passwd,
                    'db_database':  options.db_database,
                    }))
    if options.sqlite_out:
        procs.append(SqliteProcessor(args, **{
                    'db_file':      options.db_file,
                    }))
    if options.wattzon_out:
        procs.append(WattzOnProcessor(args, **{
                    'wo_api_key':   options.wo_api_key,
                    'wo_user':      options.wo_user,
                    'wo_pass':      options.wo_pass,
                    'wo_map':       options.wo_map,
                    }))
    if options.plotwatt_out:
        procs.append(PlotWattProcessor(args, **{
                    'pw_api_key':   options.pw_api_key,
                    'pw_house_id':  options.pw_house_id,
                    'pw_map':       options.pw_map,
                    }))
    if options.enersave_out:
        procs.append(EnerSaveProcessor(args, **{
                    'es_token':     options.es_token,
                    'es_url':       options.es_url,
                    'es_map':       options.es_map,
                    }))
    if options.peoplepower_out:
        procs.append(PeoplePowerProcessor(args, **{
                    'pp_url':       options.pp_url,
                    'pp_token':     options.pp_token,
                    'pp_hub_id':    options.pp_hub_id,
                    'pp_map':       options.pp_map,
                    }))
    if options.eragy_out:
        procs.append(EragyProcessor(args, **{
                    'eg_url':        options.eg_url,
                    'eg_gateway_id': options.eg_gateway_id,
                    'eg_token':      options.eg_token,
                    }))
    if options.smartenergygroups_out:
        procs.append(SmartEnergyGroupsProcessor(args, **{
                    'seg_url':      options.seg_url,
                    'seg_token':    options.seg_token,
                    'seg_map':      options.seg_map,
                    }))
    if options.thingspeak_out:
        procs.append(ThingSpeakProcessor(args, **{
                    'ts_url':      options.ts_url,
                    'ts_tokens':   options.ts_tokens,
                    'ts_fields':   options.ts_fields,
                    }))
    if options.pachube_out:
        procs.append(PachubeProcessor(args, **{
                    'pbe_url':     options.pbe_url,
                    'pbe_token':   options.pbe_token,
                    'pbe_feed':    options.pbe_feed,
                    }))

    # Data Collector setup
    if options.serial_read:
        col = SerialCollector(procs,
                              options.serial_port or SERIAL_PORT,
                              options.serial_baud or SERIAL_BAUD)

    elif options.ip_read:
        if options.ip_mode \
          and not (options.ip_mode == 'client' or options.ip_mode == 'server'):
            print 'Unknown mode %s: use client or server' % options.ip_mode
            sys.exit(1)

        mode = options.ip_mode or IP_DEFAULT_MODE
        if mode == 'server':
            col = SocketServerCollector(procs,
                                        options.ip_host or IP_HOST,
                                        options.ip_port or IP_PORT)
        else:
            col = SocketClientCollector(procs,
                                        options.ip_host or IP_HOST,
                                        options.ip_port or IP_PORT)

    elif options.db_read:
        col = DatabaseCollector(procs,
                                options.db_read_host or DB_HOST,
                                options.db_read_database or DB_DATABASE,
                                options.db_read_user or DB_USER,
                                options.db_read_passwd or DB_PASSWD)

    else:
        print 'Please specify a data source (or \'-h\' for help):'
        print '  --serial     read from serial'
        print '  --ip         read from TCP/IP'
        print '  --db         read from database'
        sys.exit(1)

    col.run()

    sys.exit(0)
