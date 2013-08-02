from __future__ import absolute_import, division

from datetime import datetime

import matplotlib.pyplot as plt

from detonator import register
from detonator.utils import timer, parse_start_end, map_data, crashes, elementary_stats

from .common import r, CrashCounts

def top_crashes_key(start, end=""):
  return "dt-top-crashes-{}-{}".format(start, end)

def _top_crashes(args):
  filename, (start, end) = args

  pipe = r.pipeline()
  for crash in crashes(filename):
    pipe.hincrby(top_crashes_key(start, end), crash.signature_id, 1)

  pipe.execute()

@register
def top_crashes(top=10, start="", end="", *args):
  start, end = parse_start_end(start, end)
  top = int(top)

  if "force" in args or not r.exists(top_crashes_key(start, end)):
    r.delete(top_crashes_key(start, end))
    with timer():
      map_data(_top_crashes, start, end, start=datetime.strptime(start, "%Y%m%d"), end=datetime.strptime(end, "%Y%m%d"))

  with timer():
    for i in sorted(r.hgetall(top_crashes_key(start, end)).items(), key=lambda x: int(x[1]), reverse=True)[:top]:
      print i

@register
def crashes_time_series(type="global", start="", end="", hours=24, *args):
  start, end = parse_start_end(start, end)
  hours = int(hours)

  cc = CrashCounts(type, hours)
  t, counts = cc.crash_counts(start, end)

  elementary_stats(counts)

  plt.title("{} crashes from {} to {} (bin={} hours)".format(type, start, end, hours))
  plt.xlabel("Time")
  plt.ylabel("Crashes")
  plt.xlim(min(t), max(t))
  plt.ylim(0, max(counts) * 1.2)
  plt.grid(True)
  plt.plot(t, counts)
  plt.show()
