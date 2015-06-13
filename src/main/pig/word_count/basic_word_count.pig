-- If you run storm-local it will launch a test cluster, the following option specifies how long to wait.
--set pig.streaming.jarfile 'basic_word_count.jar';
set pig.streaming.run.test.cluster.wait_time '60000';
--set pig.streaming.debug 'true';
set pig.streaming.spout.max.outstanding '10';

-- Pull in data.
-- Batch version:
--raw_msgs = LOAD '$text_input' USING TextLoader() AS (text:chararray);

-- Streaming version:
-- Load storm starter jar file for sentence spout.
REGISTER $stormstarterjarpath;
raw_msgs = LOAD '/dev/null' USING org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper('storm.starter.spout.RandomSentenceSpout') AS (text:chararray);

words = FOREACH raw_msgs GENERATE FLATTEN(TOKENIZE(text)) AS word;

words_gr = GROUP words BY word;

words_count = FOREACH words_gr GENERATE group, COUNT(words) AS freq;

%default output 'basic_word_count';
rmf $output;

-- Store things:

-- Batch version
--dump words_count;
--STORE words_count INTO '$output';

-- Streaming version:
explain words_count;
--STORE words_count INTO '$output' USING org.apache.pig.piggybank.squeal.backend.storm.io.SignStoreWrapper('org.apache.pig.piggybank.squeal.backend.storm.io.DebugOutput');
