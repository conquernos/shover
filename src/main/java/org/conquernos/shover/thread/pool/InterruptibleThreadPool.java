package org.conquernos.shover.thread.pool;


import org.conquernos.shover.thread.InterruptibleThreadFactory;
import org.conquernos.shover.thread.runnable.InterruptibleRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class InterruptibleThreadPool extends ThreadPoolExecutor {
	
	private List<InterruptibleRunner<?>> runners = new ArrayList<>();
	private List<Future<Object>> futures = new ArrayList<>();

	public InterruptibleThreadPool(String poolName, int poolSize) {
		super(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS
				, new LinkedBlockingQueue<>()
				, new InterruptibleThreadFactory(poolName));
	}
	
	public void execute(InterruptibleRunner<?> runner) {
		runners.add(runner);
		futures.add(submit(runner, null));
	}
	
	public void interruptAll(int millis) throws InterruptedException {
		Thread.sleep(millis);
		for (InterruptibleRunner<?> runner : runners) {
			runner.interrupt();
		}
	}

	public void joinForRunners() throws ExecutionException, InterruptedException {
		for (Future<Object> future : futures) {
			future.get();
		}
	}

}
