'''
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
'''

from utils import *

CLASS = 'DatabaseCollector'

class DatabaseCollector(BaseDataCollector):
    INSERT = True
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
    
    @staticmethod
    def make_group(cls, parser):
      group = optparse.OptionGroup(parser, 'database read options')
      group.add_option('--db', action='store_true', dest='db_read', default=False, help='read from database')
      group.add_option('--db-read-host', help='source database host')
      group.add_option('--db-read-user', help='source database user')
      group.add_option('--db-read-passwd', help='source database password')
      group.add_option('--db-read-database', help='source database name')
      parser.add_option_group(group)
    
    @staticmethod
    def is_enabled(cls, options):
      return options.db_read
    
    @staticmethod
    def help(cls):
      print '  --db         read from database'

    @staticmethod
    def make(cls, options, procs):
      if options.db_read:
          return DatabaseCollector(procs,
                                  options.db_read_host or DB_HOST,
                                  options.db_read_database or DB_DATABASE,
                                  options.db_read_user or DB_USER,
                                  options.db_read_passwd or DB_PASSWD)
      return None
