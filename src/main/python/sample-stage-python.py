#!/usr/bin/python

import sys, os, re, datetime, shutil

# read the local file from standard input
input_file=sys.stdin.readline().strip()

# make sure that we're dealing with a local file
if input_file.find("file:") != 0:
  print >> sys.stderr, "Expecting input file scheme to be 'file:': '%s'" % input_file
  sys.exit(1)

# trim away the scheme
input_file=input_file.replace("file:", "")

# make sure what we are left with is a file that exists
if not os.path.exists(input_file):
  print >> sys.stderr, "Path isn't valid: '%s'" % input_file
  sys.exit(2)

today = datetime.datetime.now()

# http://docs.python.org/library/datetime.html#strftime-strptime-behavior
date_str=today.strftime('%Y%m%d-%H%M%S')

filename = os.path.basename(input_file)

# if the filename already has a number that looks like a datetime then ignore it
if re.search(r'([0-9]{8}\-[0-9]{6})', filename) is None:
  # rename the file (add a date)
  if filename.find(".") >= 0:
    filename = filename.replace(".", "-"+date_str+".")
  else:
    filename = "%s-%s" % (filename, date_str)

new_file = "%s/%s" % (os.path.dirname(input_file), filename)

shutil.move(input_file, new_file)

new_file="file:"+new_file

# write it to standard output
print new_file,
