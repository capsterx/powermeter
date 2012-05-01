from newecm import *
from db import *

SUPPORT = 'MySql'
CLASS = "MySqlProcessor"

class MySqlProcessor(DatabaseProcessor):
    INSERT = True
    def __init__(self, *args, **kwargs):
        super(MySqlProcessor, self).__init__(*args, **kwargs)

        if not MySQLdb:
            print 'DB Error: MySQLdb module could not be imported.'
            sys.exit(1)

        self.db_host     = kwargs.get('db_host')   or DB_HOST
        self.db_user     = kwargs.get('db_user')   or DB_USER
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

    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'database options')
      group.add_option('-d', '--database', action='store_true', dest='db_out', default=False, help='write data to mysql database')
      group.add_option('--db-host', help='database host')
      group.add_option('--db-user', help='database user')
      group.add_option('--db-passwd', help='database password')
      group.add_option('--db-database', help='database name')
      parser.add_option_group(group)

    @staticmethod
    def is_enabled(options):
      return options.db_out

    @staticmethod
    def help():
      print '  --database           write to mysql database'

    @staticmethod
    def make(options, procs):
      if options.db_out:
        procs.append(MySqlProcessor(**vars(options))
