from detonator import main

import os

for root, subdir, fnames in os.walk("programs"):
  for fname in fnames:
    if fname.endswith(".py") and not fname.startswith("_"):
      # Quick hack to import the modules and automagify things.
      __import__("programs.{}".format(fname[:-3]))

if __name__ == "__main__":
  main()