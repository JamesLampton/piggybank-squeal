Overview
========

The "hello world" of cloud computing seems to be word count.  So, here you go.

First, examine `basic_word_count.pig`.  Fairly stock stuff, it would even run
locally or on a cluster with the appropriate input file.  The input data is
assumed to be simple text data.  To run the example in local mode:

    $ pig -x local -p text_input=README.md basic_word_count.pig

If you use the "dump" command, you will see something like:

    ...
    (case,5)
    (code,3)
    (data,2)
    (dump,1)
    (each,3)
    (even,1)
    (ever,1)
    ...

Now, lets switch into streaming mode.  This is where you'll need a bit more
infrastructure -- at least a Storm distribution.  For the sake of this example
we will use the RandomSentenceSpout from the [storm-starter](https://github.com/apache/incubator-storm/blob/master/examples/storm-starter/src/jvm/storm/starter/spout/RandomSentenceSpout.java) 
package (this is Storm speak in case you don't know, you may want to check out the 
[Storm Tutorial](http://storm.incubator.apache.org/documentation/Tutorial.html) 
if you're unfamiliar with these concepts).  To use it from Pig, we'll use the
`SpoutWrapper` loader:

    raw_msgs = LOAD '/dev/null' USING org.apache.pig.backend.storm.io.SpoutWrapper(
        'storm.starter.spout.RandomSentenceSpout') AS (text:chararray);

The spout wrapper expects a class name (`...RandomSentenceSpout`) along with an
optional JSON-formatted initialization string.  In this case, it is unecessary.
However, because the random sentence spout never returns an empty result and
because it has a blocking `nextTuple` function, we will need to limit the amount
of outstanding tuples so that we can get proper results withing Trident.  To
do so, we have set the maximum outstanding tuples to 10.  This is done so within
Pig-Squeal due to an [issue with Storm](https://issues.apache.org/jira/browse/STORM-368)
that I'm trying to debug.

Because we're streaming in, we will also want to stream out somehow (I think you
can still use the basic store functions but I'm not sure if I made it where they
close the file...).  Squeal comes with a debug writer:

    STORE words_count INTO '$output' USING 
        org.apache.pig.backend.storm.io.SignStoreWrapper(
            'org.apache.pig.backend.storm.io.DebugOutput');

`SignStoreWrapper` will append +1 or -1 to the output tuples based on if they
are "adding" or "subtracting" information.  We can run the newly updated code
with the data and have commented out the batch input/output statements. 

    pig -x storm-local -p stormstarterjarpath=... basic_word_count.pig
    
Now, `storm-local` (like plain `local`) will launch a test cluster.  The `DebugOutput`
store will output the results to stdout (here is a sample for the word "doctor"):

    ...
    DEBUG: (doctor,36,1)
    DEBUG: (doctor,34,-1)
    DEBUG: (doctor,37,1)
    DEBUG: (doctor,36,-1)
    DEBUG: (doctor,38,1)
    DEBUG: (doctor,37,-1)
    DEBUG: (doctor,42,1)
    DEBUG: (doctor,38,-1)
    DEBUG: (doctor,43,1)
    ...

Now, what we're seeing is that "doctor" was seen 36 times.  Wait,
scratch that, we saw it 37 times.  I'm sorry, it was 38...  Basically for
each update we send a "negative" message deleting the previously transmitted 
information possibly followed by a "positive" message.  Consumers of this information
"do the right thing" with this information and produce correct results.

This will go on for the specified test time, then the test cluster will close down.
If you want things to persist use "-x storm".  The topology will be named after the
final alias being stored (unless overridden).  However, all you will do is fill your 
logs with DEBUG statements.  If you want to send your results "downstream", switch 
to `RMQStorage`.  Before you do, be sure to setup state for your intermediate groups.

State
=====

By default, Squeal stores intermediate state using the Trident in memory LRU store.
This presents three problems: filling memory (for non-LRU memory store), lack of 
persistence, and no fault tolerance.  Trident provides a state mechanism to address
all these issues.  To enable them for Squeal, you will need to set properties to tell 
the system how to persist things.  This project includes a Redis-based state system 
and an example for our word count example.  In this example we use an external 
properties file to tell Pig what to do.  I've expanded the JSON settings to make
it easier to read:

    words_gr={
        "StateFactory": "org.apache.pig.piggybank.squeal.state.RedisState",
        "StaticMethod": "fromJSONArgs",
        "args": [
            {
                "dbNum": 1,
                "expiration": 300,
                "key_serializer": "org.apache.pig.backend.storm.state.PigTextSerializer",
                "serializer": "org.apache.pig.backend.storm.state.GZPigSerializer",
                "servers": "localhost"
            }
        ]
    }

I opted to abuse the properties functionality to minimize the changes necessary to the Pig
language and to allow code to be easily executed in batch or streaming.  In this case,
we want the alias `words_gr` to be persisted using the `RedisState` mechanism.
We have a static factory that will initialize the system using JSON arguments.  In this case
we want to store the results in db one, expire after five minutes, serialize the keys
as text, compress the values, and use localhost.  If you're running on a cluster, `RedisState`
supports sharded partitioning of keys.  Specify a comma-separated list of servers to enable
this capability.
    
Now, after re-running our test code (with `-P basic_word_count.properties`) we can check in
Redis for intermediate results:

    redis 127.0.0.1:6379[1]> keys *
     1) "nature\n"
     2) "score\n"
     3) "four\n"
     4) "a\n"
     5) "apple\n"
     6) "day\n"
     7) "with\n"
    ...

Because we compressed the values, it would be meaningless to examine them.  However, if you
ever need to debug things there is a utility routine in `StateWrapper`:

    export a_cp=$(ls $STORM_HOME/lib/*.jar $PIG_HOME/pig*.jar | xargs | tr ' ' :):$(ls target/piggybank*.jar)
    java -cp $a_cp org.apache.pig.backend.storm.state.StateWrapper \
        "$(grep words_gr_store_opts src/main/pig/word_count/basic_word_count.properties |\
            cut -f 2- -d=)" "nature"

Which produces the following arcane results (pretty printed by hand...):

    [CombineWrapperState@-711013265: [
        last=CombineTupleWritable(
            [Null: false index: 0 nature, Null: false index: 0 ((124))]), 
        cur=CombineTupleWritable(
            [Null: false index: 0 nature, Null: false index: 0 ((126))])]]

Basically, the system stores the current and last values for each key.  When a new value
arrives, these are used to produce the appropriate delta messages.  Note: `COUNT` is combinable and
invertible so it can get away with storing a single integer.  This isn't always the case and Squeal
will fall back to another mechanism that stores much more data.  It may not be fast or efficient,
but it will be correct.
