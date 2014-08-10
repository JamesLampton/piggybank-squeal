package org.apache.pig.backend.storm.io;

import java.io.IOException;

import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

public class DebugOutput extends PigStorage {
	
	@Override
	public void putNext(Tuple t) throws IOException {
		System.out.println("DEBUG: " + t);
	}
}
