package jaso.log.database;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jaso.log.protocol.Status;


public class MyCallback implements Db_LogCallback {
	private CountDownLatch latch = new CountDownLatch(1);
	private Status status = Status.ERROR;

	@Override
	public void handle(Status status) {
		this.status = status;
		latch.countDown();			
	}

	public Status getStatus() {
		return status;
	}

	public String getStatusAsString() {
		return status.toString();
	}

	public void await() {
		try {
			if(! latch.await(100, TimeUnit.SECONDS)) {
				System.err.println("Never received the callback");
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
