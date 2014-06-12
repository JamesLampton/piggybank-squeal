piggybank-squeal
================

Squeal is a means of executing Pig scripts on Storm.  However, because
things slightly different, this project provides some additional glue
routines and the quintesential "Hello world!" of map reduce: word count.

A basic explanation of how this works is available under doc/streamingpig.pdf.
However, this paper does not reference a significantly related work by Lammel
and Saile: http://softlang.uni-koblenz.de/deltamr/

This work is part of a Ph.D. thesis.  I completed most of the programming last
year but things were put on hold due to my plunge into fatherhood.  They say
if it wasn't written down, it didn't happen.  Thankfully, the open source
community provides another means of proof of existence: release the code!

Thankfully, I'm getting back to work on this.  More documentation/explanations
are forthcoming.  I don't mind answering questions, but my major focus is
completing my dissertation at this time.

NOTE: this is beta code.  I still need to finish porting my changes from pig-0.10.

Building/Installing
===================

NOTE: To ease any startup, I have included a distribution of Squeal under squeal-dist.
You will still need to deploy the pig-0.14.0-SNAPSHOT jar file to build this
code.

Install the newest version of Storm.  This was necessary due to a bug in
the current release available in maven.

    git clone https://github.com/apache/incubator-storm.git
    cd incubator-storm
    mvn install

Install Pig with Squeal:

    git clone https://github.com/JamesLampton/pig-squeal.git
    cd pig-squeal
    ant -Dhadoopversion=23 package \
        -Dforrest.home=/home/jhl1/work.local/apache-forrest-0.9/
    mvn install:install-file -Dfile=build/pig-0.14.0-SNAPSHOT.jar \
        -DgroupId=org.apache.pig -DartifactId=pig \
        -Dversion=0.14.0-SNAPSHOT -Dpackaging=jar

Build piggybank-squeal:

    mvn assembly:single

Examples
========

See src/main/pig/word\_count/README for further explanation.
