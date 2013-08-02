import csv
from datetime import datetime
import hashlib
import multiprocessing
import time
import os

import numpy as np

class timer:
  def __enter__(self):
    self.start_time = time.time()

  def __exit__(self, type, value, traceback):
    print "\033[1m\033[92mTime taken: {}\033[0m".format(round(time.time() - self.start_time, 2))

def csv_no_header(filename):
  with open(filename) as f:
    reader = csv.reader(f, delimiter="\t")
    first = True

    for row in reader:
      if first:
        first = False
        continue
      yield row

class Crash(object):
  def __init__(self, row):
    self.signature = row[0]
    self.signature_id = hashlib.sha1(self.signature).hexdigest()
    t = row[4]
    year = int(t[:4], 10)
    month = int(t[4:6], 10)
    day = int(t[6:8], 10)
    hour = int(t[8:10], 10)
    minute = int(t[10:12], 10)
    self.time = datetime(year, month, day, hour, minute)
    #self.hour = self.time.strftime("%Y-%m-%d %H")
    #self.product = None if row[29] == r"\N" else row[29]

def crashes(filename):
  i = 0
  start = time.time()
  for row in csv_no_header(filename):
    if i % 50000 == 0:
      print "Processed {} crashes from {}".format(i, filename)
    i += 1
    yield Crash(row)

  print "\033[1m\033[93mProcessed all {} crashes from {} in {} seconds\033[0m".format(i, filename, round(time.time() - start, 2))

def map_data(f, *args, **kwargs):
  callback = kwargs.get("callback", lambda x: None)

  start, end = kwargs.get("start", None), kwargs.get("end", None)
  if start:
    start = start.strftime("%Y%m%d")

  if end:
    end = end.strftime("%Y%m%d")

  pool = multiprocessing.Pool()
  files = []
  for fname in os.listdir("data"):
    if fname.endswith(".csv"):
      if (not start and not end) or ((not start or fname[:-4]) >= start and (not start or fname[:-4] <= end)):
        files.append(os.path.join("data", fname))

  try:
    results = pool.map_async(f, zip(files, [args for i in xrange(len(files))]), callback=callback)
    return results.get()
  except KeyboardInterrupt:
    pool.terminate()

def parse_start_end(start, end):
  if not start or not end:
    fs = [name for name in os.listdir("data") if name.endswith(".csv")]
    fs.sort()
    if not start:
      start = fs[0][:8]

    if not end:
      end = fs[-1][:8]

  return start, end

def elementary_stats(arr):
  print "Max: {}".format(np.max(arr))
  print "Min: {}".format(np.min(arr))
  print "Mean: {}".format(np.mean(arr))
  print "Stddev: {}".format(np.std(arr))
  print "Total: {}".format(sum(arr))
