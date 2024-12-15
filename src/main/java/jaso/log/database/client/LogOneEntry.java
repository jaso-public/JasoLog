package jaso.log.database.client;

import java.util.ArrayList;

import jaso.log.database.Db_LogPublisher;
import jaso.log.database.MyCallback;
import jaso.log.protocol.Action;
import jaso.log.protocol.DB_attribute;
import jaso.log.protocol.DB_item;

public class LogOneEntry {

	public static void main(String[] args) {
		Db_LogPublisher publisher = new Db_LogPublisher();
		
		String key = "Key";
		
		ArrayList<DB_attribute> attributes = new ArrayList<>();
		attributes.add(DB_attribute.newBuilder().setKey(key).setValue("value").build());
		DB_item item = DB_item.newBuilder().addAllAttributes(attributes).build();

		
		  //public void send(String key, Action action, DB_item item, Db_LogCallback callback) {
		
		for(int i=0; i<200; i++) {
			
			MyCallback callback = new MyCallback();	  
			publisher.send(key, Action.WRITE, item, callback);
			
			callback.await();
			System.out.println("callback status:"+callback.getStatusAsString());
		}
		
		publisher.close();

	}

}
