package jaso.log.sim;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SimLogResult implements Future<Long> {
    final SimLogRequest request;

    private final CountDownLatch done = new CountDownLatch(1);
    private volatile Long lsn = -1L;
    private volatile ExecutionException exception = null;


    public SimLogResult(SimLogRequest request) {
        this.request = request;
    }

    public void done(long assignedLsn) {
        this.lsn = assignedLsn;
        done.countDown();
    }

    public void error(ExecutionException exception) {
        this.exception = exception;
        done.countDown();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("Cancellation is not supported.");
    }

    @Override
    public boolean isCancelled() {
        return false;  // Cancellation not supported
    }

    @Override
    public boolean isDone() {
        return done.getCount() == 0;
    }

    @Override
    public Long get() throws InterruptedException, ExecutionException {
        done.await();  // Wait indefinitely for the result
        if (exception != null) throw exception;
        return lsn;
    }
    
    @Override
    public Long get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        boolean completedInTime = done.await(timeout, unit);
        if (!completedInTime) throw new TimeoutException("Timeout while waiting for result.");
        if (exception != null) throw exception;
        return lsn;
    }
}
