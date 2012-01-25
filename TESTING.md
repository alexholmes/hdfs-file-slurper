Instructions to manually test the HDFS Slurper
==============================================

## Setup

1.  Execute the instrutions in the README to build and deploy the HDFS Slurper to a node that has access to HDFS.
2.  On that node, create a local source directory, generate a random file and a MD5 hash (the hash will
be different from the output below.
<pre><code>$ mkdir -p /tmp/slurper-test/in
$ sudo dd bs=1048576 count=1 skip=0 if=/dev/sda of=/tmp/slurper-test/in/random-file
1+0 records in
1+0 records out
1048576 bytes (1.0 MB) copied, 0.071969 seconds, 14.6 MB/s
$ md5sum /tmp/slurper-test/in/random-file
969249981fa294b1273b91ec4dc3d34b  /tmp/slurper-test/in/random-file
</code></pre>
3.  Edit `conf/slurper-env.sh` and set your JAVA_HOME and HADOOP_HOME settings.
4.  Run the HDFS Slurper in standalone mode.
<pre><code>
bin/slurper.sh \
  --config-file /path/to/slurper/conf/examples/test.conf
</code></pre>
5.  Verify that the file was copied into HDFS
<pre><code>$ fs -ls /tmp/slurper-test/dest/random-file
Found 1 items
-rw-r--r--   1 user group    1048576 2012-01-17 21:09 /tmp/slurper-test/dest/random-file
</code></pre>
6.  Get the MD5 hash of the file in HDFS and verify it's the same as the original MD5 in step 2
<pre><code>$ fs -cat /tmp/slurper-test/dest/random-file | md5sum
969249981fa294b1273b91ec4dc3d34b  -
</code></pre>
