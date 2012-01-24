1.  Look into a way to stop Hadoop aborting file copy mid-stream when JVM shutdown is signalled (since we want to wait until
existing transfers are complete before allowing VM shutdown to complete).  See `org.apache.hadoop.fs.FileSystem.ClientFinalizer`.
2.  Look at using Java 7's WatchService to notify us for inbound file creation as an optimiztion for when the source file system is local
3.  Stream data to a script rather than execute it for each file being transfered
4.  Add support for a work script to generate 0..*  files to process
5.  Give the work script its own working directory which can be easily cleaned-up in the event of script failure (right now
if it fails after moving a file then we can't move it into the error directory since the old file doesn't exist)