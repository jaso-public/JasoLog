package jaso.log.common;

import java.util.ArrayList;
import java.util.Map;

import jaso.log.protocol.DB_attribute;
import jaso.log.protocol.DB_item;

public class ItemHelper {
	public static DB_item createItem( Map<String, String> values) {
		ArrayList<DB_attribute> attributes = new ArrayList<>();
		
		values.forEach((key, value) -> {
		    attributes.add(DB_attribute.newBuilder().setKey(key).setValue(value).build());
		});
		
		return DB_item.newBuilder().addAllAttributes(attributes).build();
	}
	
	public static void populateResult(DB_item item,  Map<String, String> result) {
		item.getAttributesList().forEach((attribute) -> {
			result.put(attribute.getKey(), attribute.getValue());
		});
	}

}
