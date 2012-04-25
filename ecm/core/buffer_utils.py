from newecm import *
from ecm_decoder import *
from utils import *
from collector import *
#### END HEADER

# Buffer Classes

class MovingBuffer(object):
    '''Maintain fixed-size buffer of data over time'''
    def __init__(self, max_timeframe=DAY):
        self.time_points  = []
        self.max_timeframe  = max_timeframe

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
