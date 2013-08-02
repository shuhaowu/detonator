import sys

class _programs:
  pass

def unavailable(*args):
  print "The command you entered is not available."
  print "Available commands are: "
  for n in dir(_programs):
    if not n.startswith("_"):
      print " -", n

  print
  print "Type python run.py help <command name> for help on specific commands."

def main():
  if len(sys.argv) > 1:
    getattr(_programs, sys.argv[1], unavailable)(*sys.argv[2:])
  else:
    unavailable()

def register(f):
  setattr(_programs, f.__name__, staticmethod(f))
  return f

@register
def help(name):
  """Prints a help message for a specific command if available."""
  f = getattr(_programs, name, None)
  if f:
    print f.__doc__ or "No help available."
  else:
    unavailable()

