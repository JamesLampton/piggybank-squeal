package org.apache.pig.piggybank.squeal.backend.storm.state;

import java.util.Collection;

public interface IUDFExposer {

	Collection<? extends String> getUDFs();

}
