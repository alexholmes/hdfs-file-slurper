A simple utility to copy files from a local file system into HDFS, and vice-versa
=================================================================================

## Motivation

Many projects require an automated mechanism to copy files between HDFS from local disk.  You can either
roll your own code, or use something like Flume which may be overkill if that's your sole usage.
This is a light-weight utility which simply copies all the files in a source directory into a destination directory.
The source or destination directories can be local, HDFS, or any other Hadoop FileSystem.

## Features

* After a successful file copy you can either remove the source file, or have it moved into another directory.
* Destination files can be compressed as part of the write codec with any compression codec which extends `org.apache.hadoop.io.compress.CompressionCodec`.
* Capability to write "done" file after completion of copy
* Verify destination file post-copy with CRC32 checksum comparison with source
* Ignores hidden files (filenames that start with ".")
* Customizable destination via a script which can be called for every source file.  Or alternatively let the utility
know a single destination directory
* Customizable pre-processing of file prior to transfer via script
and all files are copied into that location.
* A daemon mode which is compatible with `inittab` respawn
* Multi-threaded data transfer

## Important Considerations

When using this utility, as well as in general when dealing with the automated transfer of files, it's probably
worth bearing the following items in mind.

* Files must be moved into the source directory, which is an atomic operation in Linux and HDFS.  If files are copied or
written directly in the source directory the result of the slurper is undetermined.  The caveat here is that
you can write to a hidden file in the directory (filenames that start with ".") which will be ignored, and after the
write is complete remove the leading period from the filename at which point it will be copied next time the script runs.
* Make sure your filenames are globally unique to avoid name collisions in the destination file system.
* Ideally write a custom script to map the source files into a destination directory using a data partitioning scheme that makes
 sense for your data.  For example if you are moving log files into HDFS, then you may want to extract the date/time from
 the filename and write all files for a given day into a separate directory.
*  If your files are small in size then you may want to consider aggregating them together.  HDFS and MapReduce don't
work well with large numbers of small files.  This utility doesn't currently support such aggregation.
* Subdirectories and their contents aren't currently supported
* All paths must all be in HDFS URI form, with a scheme.  For example /tmp on the local
 filesystem would be `file:/tmp`, and /app in HDFS would be `hdfs:/app` (assuming you wanted to use the default NameNode and
 port settings defined in `core-site.xml` - if you didn't the URI can contain the hostname and port of a different Hadoop cluster).

## License

Apache licensed.

## Usage

To get started, simply:

1. Download, and run `mvn package`
2. Copy the generated tarball under `target/` to a machine that has access to Hadoop, and untar.
3. Edit `conf/slurper-env.sh` to set your Java and Hadoop home directories
4. Edit `conf/slurper.conf` and set the properties for your environment
5. Run!


### Key Settings in the Slurper Properties File

The required properties which you'll need to set in `conf/slurper.conf` are:

1.  "DATASOURCE_NAME", a logical name for the data being transferred.
2.  "SRC_DIR", which is the source directory for the Slurper.
3.  "WORK_DIR", which is the where files from the source directory are moved prior to copying them to the destination.
4.  "COMPLETE_DIR", which is where files are moved once the copy has succeeded successfully.
5.  "ERROR_DIR", where files are moved if the copy failed.
6.  "DEST_STAGING_DIR", the staging directory on the destination file system where the file is copied.  When the copy
completes files are then moved into "DEST_DIR".
7.  "DEST_DIR", the destination directory for files.


### Example 1

To copy from the localhost directory `/tmp/slurper/in` to the HDFS directory `/incoming/`.  Note that the Hadoop URI is used, which is a
 requirement for all paths.

<pre><code>shell$ cat conf/examples/basic.conf
DATASOURCE_NAME = test
SRC_DIR = file:/tmp/slurper/in
WORK_DIR = file:/tmp/slurper/work
COMPLETE_DIR = file:/tmp/slurper/complete
ERROR_DIR = file:/tmp/slurper/error
DEST_STAGING_DIR = hdfs:/incoming/stage
DEST_DIR = hdfs:/incoming
</code></pre>

Run the Slurper in foreground mode in a console:

<pre><code>shell$ bin/slurper.sh \
  --config-file /path/to/slurper/conf/examples/basic.conf
</code></pre>

In another console create an empty file and watch the Slurper do its stuff:

<pre><code>shell$ echo "test" > /tmp/slurper/in/test.txt
</code></pre>


### Example 2

Same directories as Example 1, but LZOP-compress and index files as they are written to the destination.
We're also verifying the write, which reads the file after the write has completed and compares the
checksum with the original file.  This can work with or without compression.

<pre><code>shell$ cat conf/examples/lzop-verify.conf
DATASOURCE_NAME = test
SRC_DIR = file:/tmp/slurper/in
WORK_DIR = file:/tmp/slurper/work
COMPLETE_DIR = file:/tmp/slurper/complete
ERROR_DIR = file:/tmp/slurper/error
DEST_STAGING_DIR = hdfs:/incoming/stage
DEST_DIR = hdfs:/incoming
COMPRESSION_CODEC = com.hadoop.compression.lzo.LzopCodec
CREATE_LZO_INDEX = true
VERIFY = true
</code></pre>

The slurper can be executed in the same fashion as we saw in Example 1.

### Example 3

In our final example we can use the same local source directory, but use it in conjunction with
a script to dynamically map the source filename to a HDFS directory.

The source filename in URI form will be supplied to the standard input of the script, and the
script should produce the target destination file in URI form on standard output as a single line.

The script below, which is in Python (but can be any executable), extracts the date from the filename and
uses it to write into a HDFS directory `/data/YYYY/MM/DD/<original-filename>`.

<pre><code>shell$ cat bin/sample-python.py
#!/usr/bin/python

import sys, os, re

# read the local file from standard input
input_file=sys.stdin.readline()

# extract the filename from the file
filename = os.path.basename(input_file)

# extract the date from the filename
match=re.search(r'([0-9]{4})([0-9]{2})([0-9]{2})', filename)

year=match.group(1)
mon=match.group(2)
day=match.group(3)

# construct our destination HDFS file
hdfs_dest="hdfs:/data/%s/%s/%s/%s" % (year, mon, day, filename)

# write it to standard output
print hdfs_dest,
</code></pre>

Our configuration needs to include the absolute path to the Python script.  Note too that we don't
define "DEST_DIR", since it and "SCRIPT" are mutually exclusive.

<pre><code>shell$ cat conf/examples/dynamic-dest.conf
DATASOURCE_NAME = test
SRC_DIR = file:/tmp/slurper/in
WORK_DIR = file:/tmp/slurper/work
COMPLETE_DIR = file:/tmp/slurper/complete
ERROR_DIR = file:/tmp/slurper/error
DEST_STAGING_DIR = hdfs:/incoming/stage
SCRIPT = /path/to/slurper/bin/sample-python.py
</code></pre>


And you would use it as follows:

<pre><code>shell$ touch /tmp/slurper/in/apache-20110202.log

shell$ bin/slurper.sh \
  --config-file /path/to/slurper/conf/examples/basic.conf

Launching script '/tmp/hdfs-file-slurper/src/main/python/sample-python.py' and piping the following to stdin 'file:/tmp/slurper/work/apache-20110202.log'
Copying source file 'file:/tmp/slurper/work/apache-20110202.log' to staging destination 'hdfs:/incoming/stage/675861557'
Attempting creation of target directory: hdfs:/data/2011/02/02
Local file size = 0, HDFS file size = 0
Moving staging file 'hdfs:/incoming/stage/675861557' to destination 'hdfs:/data/2011/02/02/apache-20110202.log'
File copy successful, moving source file:/tmp/slurper/work/apache-20110202.log to completed file file:/tmp/slurper/complete/apache-20110202.log
</code></pre>

