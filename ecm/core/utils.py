import time
from utils import *
#### END HEADER

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

def getgmtime():
    return int(time.time())
