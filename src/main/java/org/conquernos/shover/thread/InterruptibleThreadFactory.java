package org.conquernos.shover.thread;


import org.conquernos.shover.thread.runnable.InterruptibleRunner;


import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class InterruptibleThreadFactory implements ThreadFactory {

    private final ThreadGroup group;
    private final String threadPrefix;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public InterruptibleThreadFactory(ThreadGroup group) {
    	this.group = group;
    	this.threadPrefix = group.getName();
    }

    public InterruptibleThreadFactory(String groupName) {
    	this.group = new ThreadGroup(groupName);
    	this.threadPrefix = group.getName();
    }

    public InterruptibleThread newThread(InterruptibleRunner<?> runner) {
        InterruptibleThread thread = new InterruptibleThread(group, runner, getThreadName());
        thread.setDaemon(false);
        thread.setPriority(Thread.NORM_PRIORITY);
        return thread;
    }

	public Thread newThread(Runnable runner) {
        Thread thread = new Thread(group, runner, getThreadName());
        thread.setDaemon(false);
        thread.setPriority(Thread.NORM_PRIORITY);
        return thread;
	}
	
	private String getThreadName() {
        return threadPrefix + "-" + threadNumber.getAndIncrement();
	}

}
