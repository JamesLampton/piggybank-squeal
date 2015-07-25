-- If you run storm-local it will launch a test cluster, the following option specifies how long to wait.
set pig.streaming.run.test.cluster.wait_time '60000';
set pig.streaming.extra.conf 'test_hook.yaml';
set pig.streaming.workers '4';

-- Register the helper functions.

-- Register the normal piggybank.
%default piggybankpath '/opt/pig/contrib/piggybank/java/piggybank.jar'
REGISTER $piggybankpath;

%default rate '100';
%default size '1024';

-- Run the performance spout.
raw_msgs = LOAD '/dev/null' USING org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper('org.apache.pig.piggybank.squeal.spout.TestRateSpout', '["$rate", "$size"]', '1');

-- Add an intermediate step to cause a transfer of the raw stuff without entering the storage mechanism
set raw_msgs_shuffleBefore 'true';
set raw_msgs_parallel '4';
raw_msgs = FOREACH raw_msgs GENERATE lastMinute;

--describe raw_msgs;
gr = GROUP raw_msgs BY lastMinute;
c = FOREACH gr GENERATE group, COUNT(raw_msgs);

%default output 'perfoutput';
rmf $output;

--explain c;
--STORE c INTO '$output' USING org.apache.pig.piggybank.squeal.backend.storm.io.SignStoreWrapper('org.apache.pig.piggybank.squeal.backend.storm.io.DebugOutput', '["false"]');
STORE c INTO '$output' USING org.apache.pig.piggybank.squeal.backend.storm.io.SignStoreWrapper('org.apache.pig.piggybank.squeal.backend.storm.io.DebugOutput');
