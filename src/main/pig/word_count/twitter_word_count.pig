-- If you run storm-local it will launch a test cluster, the following option specifies how long to wait.
set pig.streaming.run.test.cluster.wait_time '60000';
--set pig.streaming.extra.conf 'test_hook.yaml';

-- Register the helper functions.

DEFINE JSONPath org.apache.pig.piggybank.evaluation.JSONPath();

-- Register the normal piggybank.
%default piggybankpath '/opt/pig/contrib/piggybank/java/piggybank.jar'
REGISTER $piggybankpath;
%default time_format 'EEE MMM dd HH:mm:ss Z yyyy';

DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToMinute org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToMinute();
DEFINE LENGTH org.apache.pig.piggybank.evaluation.string.LENGTH();

-- Pull in data.
-- Batch version:
--raw_msgs = LOAD 'sample.gz' USING TextLoader() AS (msg:chararray);

--%default rmqup 'guest:guest@'
%default rmqup 'twitterspout:guest'
%default rmqserver 'rabbit'
%default vhost 'twitter'
%default exch 'testfeeder'

-- Streaming version:
%default msgOutstanding '250';
raw_msgs = LOAD '/dev/null' USING org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper('org.apache.pig.piggybank.squeal.spout.RMQSpout', '["amqp://$rmqup@$rmqserver/$vhost", "$exch", "basic_word_count", "$msgOutstanding"]', '4') AS (msg:chararray);

-- Pull out the tweet JSON/source.
tweets = FOREACH raw_msgs GENERATE FLATTEN(STRSPLIT(msg, '\\t', 2)) AS (tweet_json:chararray, source:int);
parsed_tweets = FOREACH tweets GENERATE JSONPath('created_at text', tweet_json);
parsed_tweets = FILTER parsed_tweets BY $0#'error' IS NULL AND $0#'created_at' IS NOT NULL AND $0#'created_at' != '';
parsed_tweets = FOREACH parsed_tweets GENERATE $0#'created_at' AS created_at, TOKENIZE($0#'text') AS words;

words = FOREACH parsed_tweets GENERATE 
    ISOToMinute(CustomFormatToISO(created_at, '$time_format')) AS timestamp,
    FLATTEN(words) AS word;
words = FILTER words BY LENGTH(word) > 3;
words = FOREACH words GENERATE timestamp, LOWER(word) AS word;

words_time_gr = GROUP words BY (word, timestamp) PARALLEL 4;
words_count = FOREACH words_time_gr GENERATE FLATTEN(group), COUNT(words) AS freq;
words_count = FILTER words_count BY freq > 4;

counts_hist_gr = GROUP words_count BY (timestamp, freq);
counts_hist = FOREACH counts_hist_gr GENERATE FLATTEN(group), COUNT(words_count) AS freq_count;

%default output 'basic_word_count';
rmf $output;

-- Store things:

-- Batch version
--STORE words_count INTO '$output';

-- Streaming version:
--STORE words_count INTO '$output' USING org.apache.pig.piggybank.squeal.backend.storm.io.SignStoreWrapper('org.apache.pig.piggybank.squeal.backend.storm.io.DebugOutput');
STORE words_count INTO '$output' USING org.apache.pig.piggybank.squeal.backend.storm.io.SignStoreWrapper('org.apache.pig.piggybank.storage.RMQStorage', 'amqp://$rmqup@$rmqserver/twitter', '$exch-words');
