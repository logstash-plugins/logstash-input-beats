package org.logstash.beats.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DaemonThreadFactory implements ThreadFactory {

    final ThreadGroup group;
    final AtomicInteger threadNumber = new AtomicInteger(1);
    final String namePrefix;

    DaemonThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix;
        group = Thread.currentThread().getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r,
                namePrefix + "[T#" + threadNumber.getAndIncrement() + "]",
                0);
        t.setDaemon(true);
        return t;
    }

    public static ThreadFactory daemonThreadFactory(String namePrefix) {
        return new DaemonThreadFactory(namePrefix);
    }

}
