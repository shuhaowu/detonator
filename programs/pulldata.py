from __future__ import absolute_import

import urllib2
from datetime import datetime, timedelta
import os

from detonator import register
from detonator.utils import timer, map_data, crashes

from .common import r, buckify, CrashCounts

@register
def fetch_data(start, end):
  """Fetch data from a date to another. The date format is YYYYMMDD."""
  start = datetime(int(start[:4], 10), int(start[4:6], 10), int(start[6:8], 10))
  end = datetime(int(end[:4], 10), int(end[4:6], 10), int(end[6:8], 10))

  with timer():
    while start <= end:
      print "Downloading data from {}-{}-{}".format(start.year, start.month, start.day)
      s = start.strftime("%Y%m%d")
      url = "https://crash-analysis.mozilla.com/crash_analysis/{date}/{date}-pub-crashdata.csv.gz".format(date=s)
      req = urllib2.urlopen(url)
      with open("data/{}.csv.gz".format(s), "w") as f:
        f.write(req.read())

      os.system("gunzip data/{}.csv.gz".format(s))

      start += timedelta(1)

PROCESSED_KEY = "dates-processed"

@register
def nuke_it():
  if raw_input("Are you sure you wanna nuke it? [yN] ").lower() == "y":
    pipe = r.pipeline()
    print "\033[91m\033[1m[NUCLEAR LAUNCH DETECTED]\033[0m"
    with timer():
      for key in r.keys("dt-*"):
        pipe.delete(key)

      pipe.delete(PROCESSED_KEY)
      pipe.execute()


def _aggregate(args):
  filename, (hours, ) = args
  uid = filename + "~" +str(hours)

  if r.sismember(PROCESSED_KEY, uid):
    print "{} with {} hours already processed. Carrying on...".format(filename, hours)
    return

  gcc = CrashCounts("global", hours)
  ccs = {}

  for crash in crashes(filename):
    bucket = buckify(hours, crash.time)
    gcc.incr(bucket, 1)
    cc = ccs.get(crash.signature_id)
    if not cc:
      ccs[crash.signature_id] = cc = CrashCounts(crash.signature_id, hours)
    cc.incr(bucket, 1)

  for cc in ccs.itervalues():
    cc.finalize()

  gcc.finalize()

  r.sadd(PROCESSED_KEY, uid)

@register
def aggregate(window=24, *args):
  if "force" in args:
    nuke_it()

  window = int(window)
  with timer():
    map_data(_aggregate, window)
