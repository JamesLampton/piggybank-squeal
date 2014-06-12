Overview
========

The "hello world" of cloud computing seems to be word count.  So, here you go.

First, examine `basic_word_count.pig`.  Fairly stock stuff, it would even run
locally or on a cluster with the appropriate input file.  The input data is
assumed to be Twitter JSON objects from their API.  I'm not at liberty to
share any of my data for a sample, so you're on your own.  There is one twist:
I mark my tweets with a source (some are from the sample hose, some are from filters)
this allows me to compare the various sample sets to determine if there is
a significant difference.

To run the example in local mode (assuming you have sample.gz with some data):

    $ pig -x local 
        -p piggybankpath=$PIG_HOME/contrib/piggybank/java/piggybank.jar \
            basic_word_count.pig

Data would be stored in `basic_word_count`:

    $ head basic_word_count/part-r-00000
    #wcw    2014-05-01T04:07:00.000Z        5
    #wcw    2014-05-01T04:08:00.000Z        7
    #wcw    2014-05-01T04:09:00.000Z        7
    &gt;    2014-05-01T04:07:00.000Z        7
    &lt;    2014-05-01T04:05:00.000Z        5
    &lt;    2014-05-01T04:06:00.000Z        21
    ...

Now, lets switch into streaming mode.  This is where you'll need a bit more
infrastructure.  At this time, I have a RabbitMQ server hosting my experiment
feeds.  So, this project includes that spout (this is Storm speak in case you
don't know, you may want to check out the 
[Storm Tutorial](http://storm.incubator.apache.org/documentation/Tutorial.html) 
if you're unfamiliar with these concepts).  To use it from Pig, we'll use the
`SpoutWrapper` loader:

    raw_msgs = LOAD '/dev/null' USING org.apache.pig.backend.storm.io.SpoutWrapper(
        'org.apache.pig.piggybank.squeal.spout.RMQSpout', 
            '["amqp://$rmqup@$rmqserver/twitter", "$exch", "basic_word_count", 
            "$msgOutstanding"]') AS (msg:chararray);

The spout wrapper expects a class name (`...RMQSpout`) along with a JSON-formatted
initialization string.  In this case, we will be connecting to the specified
RabbitMQ server and pulling from the specified exchange.  We will create a queue
`basic_word_count` if necessary or subscribe to the existing queue.  Finally,
we will allow `$msgOutstanding` messages to be sent out from each spout.

Because we're streaming in, we will also want to stream out somehow (I think you
can still use the basic store functions but I'm not sure if I made it where they
close the file...).  Squeal comes with a debug writer:

    STORE words_count INTO '$output' USING 
        org.apache.pig.backend.storm.io.SignStoreWrapper(
            'org.apache.pig.backend.storm.io.DebugOutput');

`SignStoreWrapper` will append +1 or -1 to the output tuples based on if they
are "adding" or "subtracting" information.  We can run the newly updated code
(assuming you have RabbitMQ with the data and have commented out the batch
input/output statements.  TODO: create a simple debug spout...):

    pig -x storm-local -p piggybankpath=... -p rmqup=user:passwd \
        -p rmqserver=... -p exch=... basic_word_count.pig
    
Now, `storm-local` (like plain `local`) will launch a test cluster.  The `DebugOutput`
store will output the results to stdout (here is a sample for the word "this"):

    DEBUG: (this,2014-06-12T02:32:00.000Z,13,1)
    DEBUG: (this,2014-06-12T02:32:00.000Z,21,1)
    DEBUG: (this,2014-06-12T02:32:00.000Z,13,-1)
    DEBUG: (this,2014-06-12T02:32:00.000Z,28,1)
    DEBUG: (this,2014-06-12T02:32:00.000Z,21,-1)
    DEBUG: (this,2014-06-12T02:32:00.000Z,38,1)
    DEBUG: (this,2014-06-12T02:32:00.000Z,28,-1)

Now, what we're seeing is that "this" was seen 13 times at 2:32 GMT.  Wait,
scratch that, we saw it 21 times.  I'm sorry, it was 28...  Basically for
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

By default, Squeal stores intermediate state using the Trident in memory store (TODO: LRU or just memory?).
This presents three problems: filling memory, lack of persistence, and no fault tolerance.
Trident provides a state mechanism to address all these issues.  To enable them for Squeal,
you will need to set properties to tell the system how to persist things.  This project
includes a Redis-based state system and an example for our word count example.  In this example we
use an external properties file to tell Pig what to do.  I've expanded the JSON settings to make
it easier to read:

    words_time_gr_store_opts={
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
we want the alias `words_time_gr` to be persisted using the `RedisState` mechanism.
We have a static factory that will initialize the system using JSON arguments.  In this case
we want to store the results in db one, expire after five minutes, serialize the keys
as text, compress the values, and use localhost.  If you're running on a cluster, `RedisState`
supports sharded partitioning of keys.  Specify a comma-separated list of servers to enable
this capability.
    
Now, after re-running our test code (with `-P basic_word_count.properties`) we can check in
Redis for intermediate results:

    redis 127.0.0.1:6379[1]> keys (this,2014\*
     1) "(this,2014-06-12T02:46:00.000Z)\n"
     2) "(this,2014-06-12T02:45:00.000Z)\n"
     3) "(this,2014-06-12T02:42:00.000Z)\n"
     4) "(this,2014-06-12T02:40:00.000Z)\n"
     5) "(this,2014-06-12T02:41:00.000Z)\n"

Because we compressed the values, it would be meaningless to examine them.  However, if you
ever need to debug things there is a utility routine in `StateWrapper`:

    export a_cp=$(ls $STORM_HOME/lib/*.jar $PIG_HOME/pig*.jar | xargs | tr ' ' :):$(ls target/piggybank*.jar)
    java -cp $a_cp org.apache.pig.backend.storm.state.StateWrapper \
        "$(grep words_time_gr_store_opts src/main/pig/word_count/basic_word_count.properties |\
            cut -f 2- -d=)" "(this,2014-06-12T02:50:00.000Z)"

Which produces the following arcane results (pretty printed by hand...):

    [CombineWrapperState@-711013265: [
        last=CombineTupleWritable(
            [Null: false index: 0 (this,2014-06-12T02:50:00.000Z), Null: false index: 0 ((105))]), 
        cur=CombineTupleWritable(
            [Null: false index: 0 (this,2014-06-12T02:50:00.000Z), Null: false index: 0 ((113))])]]

Basically, the system stores the current and last values for each key.  When a new value
arrives, these are used to produce the appropriate delta messages.  Note: `COUNT` is combinable and
invertible so it can get away with storing a single integer.  This isn't always the case and Squeal
will fall back to another mechanism that stores much more data.  It may not be fast or efficient,
but it will be correct.
