from __future__ import absolute_import, division

from datetime import datetime, timedelta

from math import sqrt
from scipy import stats
import numpy as np
import matplotlib.pyplot as plt

from detonator import register

from .common import CrashCounts, buckify

def acf(x, length=8):
  a = np.array([np.corrcoef(x[i:], x[:-i])[0,1] for i in range(1, length)])
  return a

@register
def acf_plot_raw(type, date, lag=8, window=24, days=30):
  lag, window, days = int(lag), int(window), int(days)
  date = datetime.strptime(date, "%Y%m%d")

  target = int(buckify(window, date))

  cc = CrashCounts(type, window)
  t, counts = cc.crash_counts()

  target = t.index(target)
  counts = counts[target-days:target+1]
  t = [i - 0.02 for i in xrange(1, lag)]
  a = acf(counts, lag)
  plt.title("Global crash count sample ACF plot")
  plt.xlabel("Lag")
  plt.ylabel("ACF")
  plt.bar(t, a, width=0.04)

  err = 2 / sqrt(days)
  plt.plot([0, lag+1], [err, err], "r--")
  plt.plot([0, lag+1], [-err, -err], "r--")
  plt.plot([0, lag+1], [0, 0], "k-")

  plt.xlim(0, lag)
  plt.show()

@register
def lagged_scatter_plot(type, date, lag=1, window=24, days=20):
  lag, window, days = int(lag), int(window), int(days)
  date = datetime.strptime(date, "%Y%m%d")

  target = int(buckify(window, date))
  cc = CrashCounts(type, window)
  t, counts = cc.crash_counts(start=date - timedelta(days), end=date)
  lagged = counts[:-lag]
  counts = counts[lag:]

  slope, intercept, r_value, p_value, stderr = stats.linregress(lagged, counts)
  print "y={}x+{}  where r={}".format(slope, intercept, r_value)

  l = sorted(lagged)

  plt.title("Lagged ({}) scatter plot".format(lag))
  plt.xlabel("count(t-{})".format(lag))
  plt.ylabel("count(t)")
  plt.plot(counts, lagged, "ro")
  plt.plot([l[0], l[-1]], [l[0] * slope + intercept, l[-1] * slope + intercept])
  plt.show()
