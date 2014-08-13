piggybank-squeal
================

Squeal is a means of executing Pig scripts on Storm.  However, because
things slightly different, this project provides some additional glue
routines and the quintesential "Hello world!" of map reduce: word count.

A basic explanation of how this works is available under [doc/streamingpig.pdf](https://github.com/JamesLampton/piggybank-squeal/blob/master/doc/streamingpig.pdf).
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

NOTE: I opted to have all the Squeal code live in a separate jar to make
it easy to pick up and use.  It also helps me maintain a cleaner codebase
for the time being.

Download and unzip the Pig 0.13 distribution.

Build piggybank-squeal:

    git clone https://github.com/JamesLampton/piggybank-squeal.git
    cd piggybank-squeal
    mvn clean compile assembly:single

Now, you will need to set your `PIG_CLASSPATH` variable:

    export PIG_CLASSPATH=$(storm classpath):$(echo $PWD/target/piggybank-squeal-*ies.jar)

Or you can link the jar files into `$PIG_HOME/share/libs`.  Once you do so,
you will have a new `storm` and `storm-local` execution type.

Examples
========

See the [word count](https://github.com/JamesLampton/piggybank-squeal/tree/master/src/main/pig/word_count) example for further explanation.
