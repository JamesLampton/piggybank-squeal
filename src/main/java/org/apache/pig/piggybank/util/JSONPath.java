package org.apache.pig.piggybank.util;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

public class JSONPath implements Serializable {
	List<String[]> paths = new ArrayList<String[]>();
	private ObjectMapper om = new ObjectMapper();
	private JsonFactory jf = new JsonFactory();
	
	
	public JSONPath(String paths) {
		for (String s : paths.split(" ")) {
			this.paths.add(s.split("/"));
		}
	}
	
	@SuppressWarnings("unchecked")
	public List<Object> process(String json) {
		
		JsonNode node;
		try {
			node = om.readTree(json);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		List<Object> ret = new ArrayList<Object>(paths.size());
		for (String[] p : paths) {
			JsonNode cur_node = node;
			for (int i = 0; i < p.length; i++) {
				if (!cur_node.has(p[i])) {
					ret.add(null);
					break;
				}

				JsonNode next = cur_node.get(p[i]);
				//System.out.println(next);
				if (next.isContainerNode()) {
					cur_node = next;
				}
				
				if (i == p.length - 1) {
					// We're at the last thing.
					if (cur_node == next) {
						// We need to encode this thing.
						if (next.isContainerNode()) {
							StringWriter writer = new StringWriter();
							try {
								om.writeTree(jf.createJsonGenerator(writer), next);
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
							ret.add(writer.toString());
						} else {
							ret.add(null);
						}
					} else {
						if (next.isTextual()) {
							// We don't want quoted text.
							ret.add(next.getTextValue());
						} else {
							ret.add(next);
						}
					}
				} else if (cur_node != next) {
					// We're not at the end of our path and the next value is not a map.
					ret.add(null);
					break;
				}
			}
		}
		
		//System.out.println(d);
		return ret;
	}
	
	public static void main(String args[]) {
		String json = "{\"id\": 123, \"created_at\":\"4321\", \"user\":{\"screen_name\": [1,2,3]}}";

		JSONPath jsonp = new JSONPath("id created_at user/screen_name");
		
		System.out.println(jsonp.process(json));
	}
}
