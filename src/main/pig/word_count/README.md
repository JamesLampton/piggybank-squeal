
    pig -x storm-local -P basic_word_count.properties -p rmqup=username:password -p rmqserver=localhost -p exch=prodfeed -p piggybankpath=.../java/piggybank.jar basic_word_count.pig

> DEBUG: (this,2014-06-12T02:32:00.000Z,13,1)
> DEBUG: (this,2014-06-12T02:32:00.000Z,21,1)
> DEBUG: (this,2014-06-12T02:32:00.000Z,13,-1)
> DEBUG: (this,2014-06-12T02:32:00.000Z,28,1)
> DEBUG: (this,2014-06-12T02:32:00.000Z,21,-1)
> DEBUG: (this,2014-06-12T02:32:00.000Z,38,1)
> DEBUG: (this,2014-06-12T02:32:00.000Z,28,-1)


> redis 127.0.0.1:6379[1]> keys (this,2014\*
>  1) "(this,2014-06-12T02:46:00.000Z)\n"
>  2) "(this,2014-06-12T02:45:00.000Z)\n"
>  3) "(this,2014-06-12T02:42:00.000Z)\n"
>  4) "(this,2014-06-12T02:40:00.000Z)\n"
>  5) "(this,2014-06-12T02:41:00.000Z)\n"

    export a_cp=$(ls $STORM_HOME/lib/*.jar $PIG_HOME/pig*.jar | xargs | tr ' ' :):$(ls target/piggybank*.jar)
    java -cp $a_cp org.apache.pig.backend.storm.state.StateWrapper "$(grep words_time_gr_store_opts src/main/pig/word_count/basic_word_count.properties| cut -f 2- -d=)" "(this,2014-06-12T02:50:00.000Z)"
