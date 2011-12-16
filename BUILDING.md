Build Instructions
=================================================================================

# Build Requirements

* Apache Maven 3.x
* Sun Java 1.6


# Build Targets

The following build targets are available:

* `mvn install` - run the build, including tests and packaging
* `mvn site:stage` - create the documentation and test coverage reports


# How to Create a Release Package

Run `mvn install` in the top-level directory, i.e. the directory where the
`pom.xml` file resides:

    $ mvn clean install

This will start the build and also run any unit tests.

Among other things this will generate a JAR file of the HDFS file slurper
code in the sub-directory `target`:

    # Example: HDFS file slurper JAR file
    #
    target/hdfs-slurper-1.0.0-SNAPSHOT.jar

This build target will also create a ready-to-install package in `target/`.
This package contains any required dependency JAR files (e.g. Apache Commons
IO) plus the Java and non-Java code of the HDFS file slurper itself:

    # Example: HDFS file slurper package including JAR dependencies
    #
    target/hdfs-slurper-1.0.0-SNAPSHOT-package.tar.gz


# How to Generate the Documentation

In addition to the various MarkDown documentation files (with an '.md' suffix)
you can generate additional documentation such as Java API docs and test
coverage reports in HTML format.

To generate the documentation run `mvn site:stage` in the top-level directory,
i.e. the directory where the `pom.xml` file resides:

    $ mvn site:stage

This will generate the documentation (in Maven APT format) of the HDFS file
slurper.  The documentation includes Java API docs and test coverage reports
from Cobertura.

The documentation will be stored in:

    target/staging/hdfs-slurper/

Here is an overview of the documentation sections:

    # Start page
    target/staging/hdfs-slurper/index.html

    # Java API docs
    target/staging/hdfs-slurper/apidocs/index.html

    # Cobertura test coverage
    target/staging/hdfs-slurper/cobertura/index.html

    # HTML-based, cross-reference version of Java source code
    target/staging/hdfs-slurper/xref/index.html

