import redis
import time
from datetime import datetime

from detonator.utils import parse_start_end

r = redis.StrictRedis()

def buckify(window, dt):
  timestamp = int(time.mktime(dt.timetuple()))
  window *= 3600
  return str(int(timestamp // window))

def binned_crashes_key(type, hours):
  return "dt-{}-crashes-{}".format(type, hours)

class CrashCounts(object):
  def __init__(self, type, hours):
    self.type = type
    self.hours = hours
    self.key = binned_crashes_key(type, hours)
    self.pipe = r.pipeline()

  def all(self):
    return r.hgetall(self.key)

  def items(self):
    return sorted(self.all().items(), key=lambda x: x[0])

  def incr(self, hour, count):
    self.pipe.hincrby(self.key, hour, count)

  def finalize(self):
    return self.pipe.execute()

  def exists(self):
    return r.exists(self.key)

  def get(self, bin):
    count = int(r.hget(self.key, bin))
    if count is None:
      return 0
    return count

  def crash_counts(self, start="", end=""):
    if not (isinstance(start, datetime) and isinstance(end, datetime)):
      start, end = parse_start_end(start, end)
      start, end = datetime(int(start[:4]), int(start[4:6]), int(start[6:8])), datetime(int(end[:4]), int(end[4:6]), int(end[6:8]))

    start, end = int(buckify(self.hours, start)), int(buckify(self.hours, end))

    crashes = self.items()
    t = []
    crash_counts = []
    for time, count in crashes:
      time = int(time)
      if time >= start and time <= end:
        t.append(time)
        crash_counts.append(int(count))

    # First and last are skewed due to the way we collect the data.
    return t[1:-1], crash_counts[1:-1]
