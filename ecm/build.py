import sys
import glob
import os
import inspect
import re

PROCESSOR_DIR = "processors"
COLLECTOR_DIR = "collectors"

with open(sys.argv[1], 'r') as f:
  main_block = f.read()

for line in main_block.split("\n"):
  m = re.match("^#### (.*.py)$", line)
  if m:
    f = os.path.abspath(m.group(1))
    d = os.path.dirname(f)
    if d not in sys.path:
      print "Appending %s to path" % d
      sys.path.append(d)

    with open(f, 'r') as f:
      text = []
      do_insert = False
      for line in f:
        if do_insert:
          text.append(line)

        elif line == "#### END HEADER\n":
          do_insert = True
      main_block = main_block.replace(m.group(0), "".join(text))


def process(folder):
  current_folder = os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0])
  working_folder = os.path.join(current_folder, folder)
  if working_folder not in sys.path:
    sys.path.append(working_folder)
  names = glob.glob(os.path.join(folder, "*.py"))
 # names = ['plugins/wattzon.py']

  constants = [] 
  docs = []
  classes = []
  supports = []
  for name in names:
    print name
    p = __import__(os.path.basename(name)[0:-3])
    cls = getattr(p, 'CLASS', None)
    support = getattr(p, 'SUPPORT', None)
    constant = getattr(p, 'Constants', None)
    if constant:
      constant_str = "".join(inspect.getsourcelines(constant)[0][1:])
      fixed_constant_str = ""
      for line in constant_str.split("\n"):
        line = re.sub('^\s*', '', line)
        fixed_constant_str += line + "\n"
      constants.append(fixed_constant_str)
    doc = inspect.getdoc(p)
    if doc:
      docs.append("".join(doc))
    if cls:
      print "Found class %s" % cls
      classes.append("".join(inspect.getsource(getattr(p, cls))))
    if support:
      supports.append(support)
  return constants, docs, classes, supports

def do_replace(main_block, constants, docs, classes, t):
  main_block = main_block.replace(
    "##### %s Constants\n" % t,
    "##### %s Constants\n" % t+ "\n".join(constants) + "\n")
  main_block = main_block.replace(
    "#### %s documentation\n" % t,
    "\n".join(docs) + "\n")
  main_block = main_block.replace(
    "#### %s\n" % t,
    "#### %s\n" % t + "\n".join(classes) + "\n")
  return main_block

constants, docs, classes, supports = process(PROCESSOR_DIR)
supports_str = "\n".join(supports)
main_block = main_block.replace(
  "Includes support for uploading to the following services:",
  "Includes support for uploading to the following services:\n" + supports_str)
main_block = do_replace(main_block, constants, docs, classes, "Processor")
constants, docs, classes, supports = process(COLLECTOR_DIR)
main_block = do_replace(main_block, constants, docs, classes, "Collector")

with open(sys.argv[2], 'w') as f:
    f.write(main_block)

