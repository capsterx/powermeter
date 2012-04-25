from newecm import *
from db import *

SUPPORT = 'SqlLite'
CLASS = 'SqliteProcessor'

class SqliteProcessor(DatabaseProcessor):
    INSERT = True
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

    @staticmethod
    def make_group(parser):
      group = optparse.OptionGroup(parser, 'sqlite options')
      group.add_option('--sqlite', action='store_true', dest='sqlite_out', default=False, help='write data to sqlite database')
      group.add_option('--db-file', help='database filename')
      parser.add_option_group(group)

    @staticmethod
    def is_enabled(options):
      return options.sqlite_out

    @staticmethod
    def help():
      print '  --sqlite             write to sqlite database'

    @staticmethod
    def make(options, procs):
      if options.sqlite_out:
          procs.append(SqliteProcessor(**{
                      'db_file':      options.db_file,
                      }))
