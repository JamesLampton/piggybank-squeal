package org.apache.pig.piggybank.squeal.flexy.components;

import backtype.storm.topology.IComponent;

public interface ISource {

	IComponent getSpout();

	void fail(Object msgId);

	void ack(Object msgId);

}
