A simple utility to copy files from a local file system into HDFS
===================================================================

## Motivation

Many projects require an automated mechanism to copy files into HDFS from local disk.  You can either
roll your own code, or use something like Oozie which may be overkill if that's your sole usage.
This is a light-weight utility which simply copies all the files in a local directory into HDFS.

## Features

* After files are copied there are two options:  you can either choose to remove the file, or have it moved
into another directory.
* It is extensible in that you can tell it to call a script for every local file to determine the
HDFS location of the destination file.  Or alternatively let the utility know a single HDFS target directory
and all files are copied into that location.
* Destination HDFS files can be compressed as part of the write codec with any compression codec which extends `org.apache.hadoop.io.compress.CompressionCodec`.
* A dry-run mode which will simply echo the copy operations, but not execute them.
* Cron/scheduler support by use of a PID file to prevent from multiple concurrent execution.
* Capability to write "done" file after completion of copy
* Verify HDFS destination post-copy with CRC32 checksum comparison with source

## Important Considerations

When using this utility, as well as in general when dealing with the automated ingress of files into HDFS, it's probably
worth bearing the following items in mind.

* Files must be moved into the local source directory, which is an atomic operation in Linux.  If files are copied or
written to directly in the local source directory the result of the slurper is undetermined.
* Make sure your filenames are globally unique to avoid name collisions in HDFS.
* Ideally write a custom script to map the local files into HDFS directories using a data partitioning scheme that makes
 sense for your data.  For example if you are moving log files into HDFS, then you may want to extract the date/time from
 the filename and write all files for a given day into a separate directory.
*  If your files are small in size then you may want to consider aggregating them together either on the client side, or
even better on the server side.  HDFS and MapReduce don't work well with large numbers of small files.

## License

Apache licensed.

## Usage

To get started, simply:

1. Download, and run ant
2. Tarball the directory and copy to a machine that has access to Hadoop, and untar.
3. Set the `HADOOP_BIN` environment variable to refer to your local hadoop script.
4. Copy `lib/commons-exec-1.1.jar` to your Hadoop lib directory, or update `hadoop-env.sh` and add it to your `HADOOP_CLASSPATH`
5. Run!

Example environment setup:

<pre><code># CDH hadoop script location
export HADOOP_BIN=/usr/bin/hadoop

# Copy third-party JAR into CDH Hadoop lib directory
sudo cp  lib/commons-exec-1.1.jar /usr/lib/hadoop/lib/
</code></pre>

To see all the options available:

<pre><code>usage: Slurper [-c <arg>] [-d] [-i <arg>] [-n] [-o <arg>] [-r] -s <arg>
       [-t <arg>] [-v]
 -c,--compress <arg>      The compression codec class (Optional)
 -d,--dryrun              Perform a dry run - do not actually copy the
                          files into HDFS (Optional)
 -i,--script <arg>        A shell script which can be called to determine
                          the HDFS target directory.The standard input
                          will contain a single line with the source file,
                          and the script must put the HDFS target full
                          path on standard output. Either this or the
                          "hdfsdif" option must be set.
 -n,--donefile            Touch a file in HDFS after the file copy process
                          has completed.  The done filename is the HDFS
                          target file appended with ".done" (Optional)
 -o,--completedir <arg>   Local filesystem completion directory where file
                          is moved after successful copy into HDFS.
                          Either this or the "remove" option must be set.
 -r,--remove              Remove local file after successful copy into
                          HDFS.  Either this or the "completedir" option
                          must be set.
 -s,--sourcedir <arg>     Local filesystem source directory for files to
                          be copied into HDFS
 -t,--hdfsdir <arg>       HDFS target directory where files should be
                          copied. Either this or the "script" option must
                          be set.
 -v,--verify              Verify the file after it has been copied into
                          HDFS.  This is slow as it involves reading the
                          entire file from HDFS. (Optional)

</code></pre>

To run in dryrun mode, and to see what files would be copied from a local directory "/app" into a "/app2" directory in HDFS:

<pre><code>bin/hdfs-file-slurper.sh --sourcedir /app --hdfsdir /app2 --completedir /completed --dryrun
</code></pre>

Simply remove the "--dryrun" option to actually perform the copy.  After a file is copied into HDFS there are two options,
you can either supply the "--remove" option to remove the source file, or specify the "--completedir" directory into which
the file is moved.


### Compression

You can also choose to compress the HDFS output file with the "--compress" option, which takes a Hadoop CompressionCodec
class.  The default behavior is to append the codec-specific extension to the end of the destination file in HDFS.  If
you don't want this to occur, you must provide a script and specify an alternative HDFS filename.
For example to use the default (DEFLATE) compression codec in Hadoop, you would:

<pre><code>bin/hdfs-file-slurper.sh --sourcedir /app --hdfsdir /app2 --completedir /completed \
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
hdfs_dest="/data/%s/%s" % (date, filename)

# write it to standard output
print hdfs_dest,
</code></pre>

And you would use it as follows:

<pre><code>touch /app/apache-2011-02-02.log
bin/hdfs-file-slurper.sh --sourcedir /app --completedir /completed --script "/app/test.py"
INFO hdfsslurper.Slurper: Copying local file '/app/apache-2011-02-02.log' to HDFS location '/data/2011-02-02/apache-2011-02-02.log
</code></pre>

