package org.logstash.beats;

import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.SingleThreadEventExecutor;

public class CustomRejectedExecutionHandler implements RejectedExecutionHandler {

    @Override
    public void rejected(Runnable task, SingleThreadEventExecutor executor) {
        System.out.println("Requeueing the message");
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executor.execute(task);
    }
}
