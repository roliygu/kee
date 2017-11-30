import os
import sys


if len(sys.argv) != 3:
  print "Usage: %s [datafile] [configfile]" % sys.argv[0]
  sys.exit(1)

############## global param ##################
filename = sys.argv[1]
configname = sys.argv[2]
mapping_file = "%s.idmap" % filename

label_name = None
schema = []

def dump_mapping():
  with open(mapping_file, "wb") as fout:
    println_info("Dumping mapping into %s" % mapping_file)
    for k in index_map.keys():
      fout.write("%s:%d\n" % (k, index_map[k]))

def println_info(s):
  print >> sys.stderr, "INFO: %s" % s

def println_error(s):
  print >> sys.stderr, "ERROR: %s" % s

def println_warn(s):
  print >> sys.stderr, "WARN: %s" % s

with open(configname, "rb") as fin:
  println_info("Reading config from file: %s" % configname)
  for line in fin:
    line = line.strip().replace(' ', '')
    if line.startswith("label=") and label_name == None:
      println_info("Label detected as: %s" % line)
      label_name = line.split("=")[1]
    elif line.startswith("label="):
      println_error("Multiple label columns found.")
      sys.exit(1)
    else:
      println_info("Append column: %s" % line)
      schema.append(line)

if None == label_name:
  println_error("Label not specified in %s" % configname)
  sys.exit(1)

############# parsing meta #################
n_schema = len(schema)
n_line = 0
index_map = {}

def index_of(s):
  if index_map.get(s) == None:
    index_map[s] = len(index_map) + 1
  return index_map[s]

with open(filename, "rb") as fin:
  n_line += 1
  label_value = None

  def append_fea(fea_list, key, value):
    fea_list.append((index_of(key), value))

  println_info("Ingesting data from file: %s" % filename)
  for line in fin:
    fea_list = []
    items = line.strip().split(",")
    if len(items) != n_schema:
      println_warn("Column number %d of line %d inconsistent with schema len %d" % (len(items), n_line, n_schema))
      continue
    for (name, value) in zip(schema, items):
      if name == label_name:
        label_value = value
      elif name.endswith("_m"):
        tuples = value.split(";")
        for entry in tuples:
          k_v = entry.split(":")
          if len(k_v) != 2:
            println_warn("Malformed map column %s of line %d" % (name, n_line))
          else:
            key, val = k_v[0], k_v[1]
            index_key = "%s_%s" % (name, key)
            append_fea(fea_list, index_key, val)
      elif name.endswith("_d"):
        index_key = "%s_%s" % (name, value)
        append_fea(fea_list, index_key, "1")
      else:
        append_fea(fea_list, name, value)

    if None == label_value:
      println_warn("Label not specifield of line %d" % n_line)
    else:
      print >> sys.stdout, "%s %s" % (label_value, " ".join(["%s:%s" % (i[0], i[1]) for i in fea_list]))

################# dump mapping ####################
dump_mapping()