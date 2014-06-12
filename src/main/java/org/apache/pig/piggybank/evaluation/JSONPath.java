package org.apache.pig.piggybank.evaluation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class JSONPath extends EvalFunc<Map> {

	org.apache.pig.piggybank.util.JSONPath jp;
	String[] ids;
	
	@Override
	public Map exec(Tuple t) throws IOException {
		Map<String, String> ret = new HashMap<String, String>();		

//		ret.put("test", "value");
		
		if (jp == null) {
			jp = new org.apache.pig.piggybank.util.JSONPath((String) t.get(0));
			ids = ((String)t.get(0)).split(" ");
		}
		
		try {
			List<Object> proc = jp.process((String) t.get(1));
			for (int i = 0; i < ids.length; i++) {
				Object v = proc.get(i);
				ret.put(ids[i], v == null ? "" : v.toString());
			}
		} catch (Exception e) {
			ret.put("error", "Could not parse: " + t.get(1));
		}
		
		return ret;
	}

	public Schema outputSchema(Schema input) {
		try{
            Schema mapSchema = new Schema();
            mapSchema.add(new Schema.FieldSchema("value", DataType.CHARARRAY));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                                   mapSchema, DataType.MAP));
        }catch (Exception e){
           return null;
        }
	}
}
