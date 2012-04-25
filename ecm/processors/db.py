from processor import *
from ecm_decoder import *
from utils import *
#### END HEADER

class DatabaseProcessor(BaseProcessor):
    INSERT = False
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
