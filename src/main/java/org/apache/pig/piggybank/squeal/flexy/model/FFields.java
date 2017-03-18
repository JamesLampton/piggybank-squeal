package org.apache.pig.piggybank.squeal.flexy.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.pig.piggybank.squeal.flexy.components.ISource;

import backtype.storm.generated.StreamInfo;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Fields;

public class FFields {

	public FFields(String... fields) {
		// TODO Auto-generated constructor stub
	}

	public FFields(List<String> list) {
		// TODO Auto-generated constructor stub
	}

	public static FFields fieldsConcat(FFields a, FFields b) {
		
		// storm.trident.util.TridentUtils
//		public static Fields fieldsConcat(Fields... fields) {
//	        List<String> ret = new ArrayList<String>();
//	        for(Fields f: fields) {
//	            if(f!=null) ret.addAll(f.toList());
//	        }
//	        return new Fields(ret);
//	    }
		
		// TODO Auto-generated method stub
		return null;
	}

	public static FFields getSingleOutputStreamFields(ISource source) {
		// TODO Auto-generated method stub
		// storm.trident.util.TridentUtils
//		public static Fields getSingleOutputStreamFields(IComponent component) {
//	        OutputFieldsGetter getter = new OutputFieldsGetter();
//	        component.declareOutputFields(getter);
//	        Map<String, StreamInfo> declaration = getter.getFieldsDeclaration();
//	        if(declaration.size()!=1) {
//	            throw new RuntimeException("Trident only supports components that emit a single stream");
//	        }
//	        StreamInfo si = declaration.values().iterator().next();
//	        if(si.is_direct()) {
//	            throw new RuntimeException("Trident does not support direct streams");
//	        }
//	        return new Fields(si.get_output_fields());        
//	    }
		return null;
	}

	public String get(int i) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public List<String> toList() {
		return null;
	}
}
