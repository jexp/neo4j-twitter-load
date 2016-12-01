package neo4j.load.twitter;

import java.util.concurrent.*;

/**
 * @author mh
 * @since 29.11.16
 */
public class Pool {
    public static ExecutorService createPool(int threads) {
        int queueSize = threads * 25;
        return new ThreadPoolExecutor(threads / 2, threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new CallerBlocksPolicy());
//                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    static class CallerBlocksPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                try {
                    // block caller for 100ms
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    // ignore
                }
                try {
                    // submit again
                    executor.submit(r).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
