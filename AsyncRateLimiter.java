package com.seyrancom;


import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Java rate-limiting library based on token-bucket algorithm.
 * <p>
 * Usage Example
 * limiter = new AsyncRateLimiter(100, 1, TimeUnit.MINUTES);
 * limiter.tryConsume(2)'
 */
public class AsyncRateLimiter {
    private static final int CHECK_CONSUMER_INTERVAL_ML = 50;
    private final int refillSize;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private final AtomicLong tokens = new AtomicLong(0);
    private final Queue<TokenConsumer> consumerQueue = new ConcurrentLinkedQueue();

    public AsyncRateLimiterOld(final int refillSize, final int refillDelay, final TimeUnit refillTimeUnit) {
        this.refillSize = refillSize;

        executor.schedule(() -> refill(), refillDelay, refillTimeUnit);
        executor.schedule(() -> notifyConsumers(), CHECK_CONSUMER_INTERVAL_ML, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Boolean> tryConsume(final long numTokens) {
        final TokenConsumer tokenConsumer = new TokenConsumer(numTokens);
        consumerQueue.add(tokenConsumer);
        return tokenConsumer.getFuture();
    }

    synchronized private void notifyConsumers() {
        synchronized (consumerQueue) {
            while (consumerQueue.size() > 0 & tokens.get() >= consumerQueue.peek().getNumTokens()) {
                tokens.getAndUpdate(prevValue -> prevValue - consumerQueue.peek().getNumTokens());
                consumerQueue.peek().getFuture().complete(true);
                consumerQueue.poll();
            }
        }
    }

    private void refill() {
        tokens.set(refillSize);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        executor.shutdown();
        while (consumerQueue != null && consumerQueue.size() > 0) {
            consumerQueue.peek().getFuture().complete(false);
        }
    }

    private static class TokenConsumer {
        private final CompletableFuture<Boolean> future;
        private final long numTokens;

        public TokenConsumer(long numTokens) {
            this.future = new CompletableFuture<>();
            this.numTokens = numTokens;
        }

        public long getNumTokens() {
            return numTokens;
        }

        public CompletableFuture<Boolean> getFuture() {
            return future;
        }
    }
}
