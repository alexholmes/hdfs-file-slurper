A simple utility to upload files from a local file system into HDFS
===================================================================

## Motivation


## Usage

To get started, simply:

1. Download, and run ant
2. Tarball the directory and copy to a machine that has access to Hadoop, and untar.
3. Set the HADOOP_CONF_DIR environment variable to refer to your Hadoop configuration directory.
4. Run!

To see all the options available:

<pre><code>
export HADOOP_CONF_DIR=/etc/hadoop/conf
bin/hdfs-file-slurper.sh
11/10/28 23:17:53 ERROR hdfsslurper.Slurper: Could not parse command line args: Missing required options: s, o
usage: Slurper [-c] [-d] [-i <arg>] -o <arg> [-r] -s <arg> [-t <arg>] [-v]
 -c,--compress            The compression codec class (Optional)
 -d,--dryrun              Perform a dry run - do not actually copy the
                          files into HDFS (Optional)
 -i,--script <arg>        A shell script which can be called to determine
                          the HDFS target directory.The standard input
                          will contain a single line with the source file,
                          and the script must put the HDFS target full
                          path on standard output. (Optional)
 -o,--completedir <arg>   Local filesystem completion directory where file
                          is moved after successful copy into HDFS
 -r,--remove              Remove local file after successful copy into
                          HDFS (Optional)
 -s,--sourcedir <arg>     Local filesystem source directory for files to
                          be copied into HDFS
 -t,--hdfsdir <arg>       HDFS target directory where files should be
                          copied
 -v,--verify              Verify the integrity of the copy.  This is a
                          slow operation.  (Optional)
</code></pre>

To run in dryrun mode, and to see what files would be copied from a local directory "/app" into a "/app2" directory in HDFS:

<pre><code>
bin/hdfs-file-slurper.sh /app --hdfsdir /app2 --completedir /completed --dryrun
</code></pre>

Simply remove the "--dryrun" option to actually perform the copy.  After a file is copied into HDFS there are two options,
you can either supply the "--remove" option to remove the source file, or specify the "--completedir" directory into which
the file is moved.

If you want to have control over a file-by-file basis as to the destination HDFS directory and file, use the
"--script" option to specify a local executable script which

For example, a Pythong script which merely echo's out what it gets from input looks like:

<pre><code>
#!/usr/bin/python

import sys
for line in sys.stdin:
    print line,
</code></pre>

And you would use it as follows:

<pre><code>
bin/hdfs-file-slurper.sh --sourcedir test --completedir test2 --script "/app/test.py"
</code></pre>

You can also choose to compress the HDFS output file with the "--compress" option, which takes a Hadoop CompressionCodec
class.  The default behavior is to append the codec-specific extension to the end of the destination file in HDFS.  If
you don't want this to occur, you must provide a script and specify an alternative HDFS filename.
For example to use the default (DEFLATE) compression codec in Hadoop, you would:

<pre><code>
bin/hdfs-file-slurper.sh --sourcedir /app --hdfsdir /app2 --completedir /completed --dryrun  --compress org.apache.hadoop.io.compress.DefaultCodec
</code></pre>
