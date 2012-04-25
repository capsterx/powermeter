#!/usr/bin/env python -u
__version__  = '2.4.2'
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


#### Collector documentation
#### Processor documentation


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
__author__  = 'Brian Jackson; Kelvin Kakugawa; Marc MERLIN; ben; mwall'

try:
  import eventlet
  eventlet.monkey_patch(socket=True, select=True)
except:
  eventlet = None
  pass

# set skip_upload to print out what would be uploaded but do not actually do
# the upload.
SKIP_UPLOAD = 0

MINUTE  = 60
HOUR  = 60 * MINUTE
DAY  = 24 * HOUR

BUFFER_TIMEFRAME = 5*MINUTE
DEFAULT_UPLOAD_TIMEOUT = 15 # seconds
DEFAULT_UPLOAD_PERIOD = 15*MINUTE

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

##### Collector Constants
##### Processor Constants

import base64
import bisect
import new
import optparse
import socket
import select
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

#### core/utils.py
#### core/ecm_decoder.py
#### core/registry.py
#### core/config.py
#### core/buffer_utils.py
#### core/collector.py
#### Collector
#### core/processor.py
#### processors/upload.py
#### processors/db.py
#### Processor

if __name__ == '__main__':
    import inspect
    import os
    import glob
    current_folder = os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0])
    def add_path(path):
      if path not in sys.path:
        sys.path.append(path)
    core_dir = os.path.join(current_folder, "core")
    add_path(core_dir)
    processor_dir = os.path.join(current_folder, "processors")
    add_path(processor_dir)
    collector_dir = os.path.join(current_folder, "collectors")
    add_path(collector_dir)
    try:
      from collector import CollectorRegistry
      from processor import ProcessorRegistry
      from config import *
    except:
      pass
    names = glob.glob(os.path.join(processor_dir, "*.py"))
    print collector_dir
    names.extend(glob.glob(os.path.join(collector_dir, "*.py")))
    print names
    for name in names:
      try:
        f = __import__(os.path.basename(name)[0:-3])
        c = getattr(f, "Constants", None)
        if c:
          for n, v in inspect.getmembers(c):
            if n[0] != "_":
              f.__dict__[n] = v
      except:
        traceback.print_exc()
        print "Error importing %s" % name

    parser = optparse.OptionParser(version=__version__)

    parser.add_option('-c', '--config-file', dest='configfile', help='read configuration from FILE', metavar='FILE')
    parser.add_option('-q', '--quiet', action='store_true', dest='quiet', default=False, help='quiet output')
    parser.add_option('-v', '--verbose', action='store_false', dest='quiet', default=False, help='verbose output')
    parser.add_option('--debug', action='store_true', default=False, help='debug output')
    parser.add_option('-p', '--print', action='store_true', dest='print_out', default=False, help='print data to screen')
    parser.add_option('--print-map', action='store', help='map of names for printing')

    group = optparse.OptionGroup(parser, 'IP read options')
    group.add_option('--ip', action='store_true', dest='ip_read', default=False, help='read from TCP/IP source such as EtherBee')
    group.add_option('--ip-host', help='ip host')
    group.add_option('--ip-port', help='ip port')
    group.add_option('--ip-mode', help='act as client or server')
    parser.add_option_group(group)
   
    for processor in ProcessorRegistry.values():
      print processor
      processor.make_group(parser)
    print ""
    print CollectorRegistry.values()
    for collector in CollectorRegistry.values():
      print collector
      collector.make_group(parser)

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
    value = False
    for processor in ProcessorRegistry.values():
      value |= (processor.is_enabled(options) or False)
    if not (value):
        print 'Please specify one or more processing options (or \'-h\' for help):'
        for processor in ProcessorRegistry.values():
          processor.help()
        sys.exit(1)

    procs = []

    for processor in ProcessorRegistry.values():
      processor.make(options, procs)

    # Data Collector setup

    col = None
    for collector in CollectorRegistry.values():
      if collector.is_enabled(options):
        print "Collector enabled %s" % collector
        col = collector.make(options, procs)
        print col
        if col:
          break
      else:
        print "Collector not enabled %s" % collector
    else:
        print 'Please specify a data source (or \'-h\' for help):'
        print col
        sys.exit(1)

    col.run()

    sys.exit(0)
