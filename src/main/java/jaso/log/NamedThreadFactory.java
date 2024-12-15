package jaso.log;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    private final String baseName;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public NamedThreadFactory(String baseName) {
        this.baseName = baseName;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, baseName + "-" + threadNumber.getAndIncrement());
    }
}
