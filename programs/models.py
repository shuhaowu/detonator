from __future__ import absolute_import, division

from datetime import datetime

from scipy import stats, optimize
from pylab import sin
from math import pi, sqrt
from numpy import array
import numpy as np
import matplotlib.pyplot as plt

from detonator import register
from detonator.utils import elementary_stats, timer
from .common import CrashCounts, buckify, parse_start_end, unbuckify, r

class BaseModel(object):
  def __init__(self, data):
    self.data = array(data)
    self.t = array(range(len(data)))
    self.train()

  def train(self):
    pass

class Simple(BaseModel):
  MIN_COUNT = 150000
  MULTIPLIER = 4
  SMOOTHING_FACTOR = 1

  def is_explosive(self, next_value):
    if next_value >= self.MIN_COUNT:
      last_value = sum(self.data[-self.SMOOTHING_FACTOR:] + 1) / self.SMOOTHING_FACTOR
      return (next_value - last_value) / last_value >= self.MULTIPLIER

    return False

class Sinusoidal(BaseModel):
  def predict(self, t):
    pass

  def predict_next(self):
    return self.predict(self.t[-1] + 1)

  def prediction_error(self, t):
    pass

  def prediction_next_error(self):
    return self.prediction_error(self.t[-1] + 1)

  def is_explosive(self, next_value):
    pass

  def train(self):
    pass

class ARMA(BaseModel):
  def predict(self, t):
    pass

  def predict_next(self):
    return self.predict(self.t[-1] + 1)

  def prediction_error(self, t):
    pass

  def prediction_next_error(self):
    return self.prediction_error(self.t[-1] + 1)

  def is_explosive(self, next_value):
    pass

  def train(self):
    pass

def get_model(model):
  modelclass = globals().get(model)
  if not modelclass or not issubclass(modelclass, BaseModel):
    raise Exception("Model {} does not exist.".format(model))
  return modelclass

@register
def predict_values(model, type, date, training_data_length=14, days_to_predict=25, window=24):
  training_data_length, days_to_predict = int(training_data_length), int(days_to_predict)
  modelclass = get_model(model)

  cc = CrashCounts(type, window)
  x, original = cc.crash_counts()

  date = datetime.strptime(date, "%Y%m%d")
  bucket = int(buckify(window, date))
  date = x.index(bucket)

  x = []
  predicted_values = []
  actual_values = []
  residuals = []

  for i in xrange(1, days_to_predict):
    counts = original[date-training_data_length-i:date-i]
    model = modelclass(counts)
    predicted = model.predict_next()
    predicted_values.append(predicted)
    actual_values.append(original[-i])
    x.append(len(original) - i)
    residuals.append(abs((predicted - original[-i]) / original[-i] * 100))

  plt.plot(x, actual_values)
  plt.plot(x, predicted_values)
  plt.legend(("Actual", "Predicted"))
  plt.ylim(0, 1.2 * np.max(predicted_values + actual_values))
  elementary_stats(residuals)
  plt.show()

@register
def check_explosive(model, type, date, training_data_length=14, window=24):
  training_data_length, window = int(training_data_length), int(window)
  modelclass = get_model(model)

  date = datetime.strptime(date, "%Y%m%d")
  bucket = int(buckify(window, date))

  cc = CrashCounts(type, window)

  x, original = cc.crash_counts()
  date = x.index(bucket)
  counts = original[date-training_data_length:date]
  model = modelclass(counts)
  print model.is_explosive(original[date])

@register
def check_explosive_period(model, type, start="", end="", training_data_length=14, window=24):
  training_data_length, window = int(training_data_length), int(window)
  start, end = parse_start_end(start, end)
  modelclass = get_model(model)

  cc = CrashCounts(type, window)
  x, counts = cc.crash_counts(start=start, end=end)


  explosive = []

  classification = []

  for i, count in enumerate(counts):
    if i <= training_data_length:
      continue

    model = modelclass(counts[i-training_data_length:i])
    if model.is_explosive(count):
      explosive.append(x[i])
      classification.append(1)
    else:
      classification.append(0)

  print "explosive: ", [unbuckify(b) for b in explosive]
  plt.plot(x[training_data_length+1:], classification, "ro")
  plt.show()

@register
def find_explosives_everywhere(model, start="", end="", training_data_length=14, window=24):
  training_data_length, window = int(training_data_length), int(window)
  modelclass = get_model(model)
  start, end = parse_start_end(start, end)

  e = 0
  explosive = []

  with timer():
    print "Starting.."
    for key in r.keys("dt-*-crashes-*"):
      if e % 10000 == 0:
        print "{} signatures processed".format(e)
      signature = key.split("-")[1]
      cc = CrashCounts(signature, window)
      x, original = cc.crash_counts(start=start, end=end)

      for i, count in enumerate(original):
        if i <= training_data_length:
          continue

        model = modelclass(original[i-training_data_length:i])
        if model.is_explosive(count):
          explosive.append((signature, unbuckify(window, x[i])))

      e += 1

  for i in explosive:
    print i

  print "Found {} explosive events.".format(len(explosive))

