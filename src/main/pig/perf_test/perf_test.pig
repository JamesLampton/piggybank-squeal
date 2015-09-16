-- If you run storm-local it will launch a test cluster, the following option specifies how long to wait.
set pig.streaming.run.test.cluster.wait_time '60000';
set pig.streaming.extra.conf 'test_hook.yaml';
set pig.streaming.workers '4';

-- Register the helper functions.

-- Register the normal piggybank.
%default piggybankpath '/opt/pig/contrib/piggybank/java/piggybank.jar'
REGISTER $piggybankpath;

%default rate '600000';
%default size '1024';
%default numRunners '32';
--%default keySpace '2048'; -- 40.96 symbols per element.
--%default keySpace '10240'; -- 40.96 symbols per element.
%default keySpace '40960'; -- 40.96 symbols per element.

DEFINE sleepMap org.apache.pig.piggybank.evaluation.TestDelay('2.159');
DEFINE sleepReduce org.apache.pig.piggybank.evaluation.TestDelay('0.042');
DEFINE sleepState org.apache.pig.piggybank.evaluation.TestDelay('8.170');

DEFINE genBag org.apache.pig.piggybank.evaluation.TestGenerateBag('42.671', '36.722');

-- Run the performance spout.
raw_msgs = LOAD '/dev/null' USING org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper('org.apache.pig.piggybank.squeal.spout.TestRateSpout', '["$rate", "$size"]', '1');

-- Add an intermediate step to cause a transfer of the raw stuff without entering the storage mechanism
set raw_msgs_shuffleBefore 'true';
set raw_msgs_parallel '$numRunners';
raw_msgs = FOREACH raw_msgs GENERATE sleepMap(), FLATTEN(genBag()) AS key;
raw_msgs = FOREACH raw_msgs GENERATE (key % 2048) AS key;

--describe raw_msgs;
gr = GROUP raw_msgs BY key PARALLEL $numRunners;
c = FOREACH gr GENERATE group, COUNT(raw_msgs) AS count;
c = FOREACH c GENERATE *, sleepReduce(), sleepState();

%default output 'perfoutput';
rmf $output;

--explain c;
STORE c INTO '$output' USING org.apache.pig.piggybank.squeal.backend.storm.io.SignStoreWrapper('org.apache.pig.piggybank.squeal.backend.storm.io.DebugOutput', '["false"]');
--STORE c INTO '$output' USING org.apache.pig.piggybank.squeal.backend.storm.io.SignStoreWrapper('org.apache.pig.piggybank.squeal.backend.storm.io.DebugOutput');
