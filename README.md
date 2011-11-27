A simple utility to copy files from a local file system into HDFS, and vice-versa
=================================================================================

## Motivation

Many projects require an automated mechanism to copy files between HDFS from local disk.  You can either
roll your own code, or use something like Flume which may be overkill if that's your sole usage.
This is a light-weight utility which simply copies all the files in a source directory into a destination directory.
The source or destination directories can be local, HDFS, or any other Hadoop FileSystem.
It can be easily run from cron.

## Features

* After a successful file copy you can either remove the source file, or have it moved into another directory.
* Destination files can be compressed as part of the write codec with any compression codec which extends `org.apache.hadoop.io.compress.CompressionCodec`.
* A dry-run mode which will simply echo the copy operations, but not execute them.
* Cron/scheduler support by use of a PID file to prevent from multiple concurrent execution.
* Capability to write "done" file after completion of copy
* Verify destination file post-copy with CRC32 checksum comparison with source
* Ignores hidden files (filenames that start with ".")
* It is extensible in that you can tell it to call a script for every local file to determine the
location of the destination file.  Or alternatively let the utility know a single destination directory
and all files are copied into that location.

## Important Considerations

When using this utility, as well as in general when dealing with the automated ingress of files, it's probably
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
work well with large numbers of small files.
* Subdirectories and their contents aren't currently supported
* Source, complete and destination paths must all be in HDFS URI form, with a scheme.  For example /tmp on the local
 filesystem would be `file:/tmp`, and /app in HDFS would be `hdfs:/app` (assuming you wanted to use the default NameNode and
 port settings defined in `core-site.xml`.

## License

Apache licensed.

## Usage

To get started, simply:

1. Download, and run ant
2. Tarball the directory and copy to a machine that has access to Hadoop, and untar.
3. Set the `HADOOP_BIN` environment variable to refer to your local hadoop script (not required if you are running a packaged version of CDH).
4. Run!

Example environment setup:

<pre><code># CDH hadoop script location
export HADOOP_BIN=/usr/bin/hadoop
</code></pre>

To see all the options available:

<pre><code>usage: Slurper [-c <arg>] [-d] [-i <arg>] [-n] [-o <arg>] [-r] -s <arg>
       [-t <arg>] [-v]
 -c,--compress <arg>       The compression codec class (Optional)
 -d,--dry-run              Perform a dry run - do not actually copy the
                           files (Optional)
 -i,--script <arg>         A shell script which can be called to determine
                           the destination directory.The standard input
                           will contain a single line with the fully
                           qualified URI of the source file, and the
                           script must put the destination  full path on
                           standard output. This must be a fully-qualified
                           URI.  For example, for a local  /tmp directory,
                           this would be file:/tmp.  For a /tmp directory
                           in HDFS, this would be
                           hdfs://localhost:8020/tmp or hdfs:/tmp if you
                           wanted to use the NameNode host and port
                           settings in your core-site.xml file.  Either
                           this or the "hdfsdif" option must be set.
 -n,--create-done-file     Touch a file in the destination directory after
                           the file copy process has completed.  The done
                           filename is the same as the destination file
                           appended with ".done" (Optional)
 -o,--complete-dir <arg>   Completion directory where file is moved after
                           successful copy.  Must be in the same
                           filesystem as the source file.  Either this or
                           the "remove" option must be set.
 -r,--remove-after-copy    Remove the source file after a successful copy.
                           Either this or the "completedir" option must be
                           set.
 -s,--src-dir <arg>        Source directory.  This must be a
                           fully-qualified URI.  For example, for a local
                           /tmp directory, this would be file:/tmp.  For a
                           /tmp directory in HDFS, this would be
                           hdfs://localhost:8020/tmp or hdfs:/tmp if you
                           wanted to use the NameNode host and port
                           settings in your core-site.xml file.
 -t,--dest-dir <arg>       Destination directory where files should be
                           copied. Either this or the "script" option must
                           be set. This must be a fully-qualified URI.
                           For example, for a local  /tmp directory, this
                           would be file:/tmp.  For a /tmp directory in
                           HDFS, this would be hdfs://localhost:8020/tmp
                           or hdfs:/tmp if you wanted to use the NameNode
                           host and port settings in your core-site.xml
                           file.
 -v,--verify               Verify the file after it has been copied.  This
                           is slow as it involves reading the entire
                           destination file after the copy has completed.
                           (Optional)
</code></pre>

To run in dryrun mode, and to see what files would be copied from a local directory "/app" into a "/app2" directory in HDFS:

<pre><code>bin/hdfs-file-slurper.sh --src-dir file:/app --dest-dir hdfs:/app2 --complete-dir file:/completed --dryrun
</code></pre>

Simply remove the "--dryrun" option to actually perform the copy.  After a file is copied into HDFS there are two options,
you can either supply the "--remove" option to remove the source file, or specify the "--complete-dir" directory into which
the file is moved.


### Compression

You can also choose to compress the HDFS output file with the "--compress" option, which takes a Hadoop CompressionCodec
class.  The default behavior is to append the codec-specific extension to the end of the destination file in HDFS.  If
you don't want this to occur, you must provide a script and specify an alternative HDFS filename.
For example to use the default (DEFLATE) compression codec in Hadoop, you would:

<pre><code>bin/hdfs-file-slurper.sh --src-dir file:/app --dest-dir hdfs:/app2 --complete-dir file:/completed \
--compress org.apache.hadoop.io.compress.DefaultCodec
</code></pre>

### Fine-grained control over HDFS file destinations

If you want to have control on a file-by-file basis as to the destination HDFS directory and file, use the
"--script" option to specify a local executable script.  The local filename will be supplied to the standard input
of the script, and the script should produce the target HDFS destination file on standard output as a single line.

For example, this is a simple Python script which uses the date in the filename to partition files into separate
directories in HDFS by date.

<pre><code>#!/usr/bin/python

import sys, os, re

# read the local file from standard input
input_file=sys.stdin.readline()

# extract the filename from the file
filename = os.path.basename(input_file)

# extract the date from the filename
date=re.search(r'([0-9]{4}\-[0-9]{2}\-[0-9]{2})', filename).group(1)

# construct our destination HDFS file
hdfs_dest="hdfs:/data/%s/%s" % (date, filename)

# write it to standard output
print hdfs_dest,
</code></pre>

And you would use it as follows:

<pre><code>touch /app/apache-2011-02-02.log
bin/hdfs-file-slurper.sh --src-dir file:/app --complete-dir /completed --script "/app/hdfs-file-slurper/sample-python.py"
INFO hdfsslurper.Slurper: Copying source file 'file:/app/apache-2011-02-02.log' to destination 'hdfs:/data/2011-02-02/apache-2011-02-02.log
</code></pre>

