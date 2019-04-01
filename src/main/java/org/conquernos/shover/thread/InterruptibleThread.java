package org.conquernos.shover.thread;


import org.conquernos.shover.thread.runnable.InterruptibleRunner;


public class InterruptibleThread extends Thread {

	protected InterruptibleRunner<?> runner = null;

	public InterruptibleThread(InterruptibleRunner<?> runner) {
		super(runner);
		this.runner = runner;
	}

	public InterruptibleThread(InterruptibleRunner<?> runner, String name) {
		super(runner, name);
		this.runner = runner;
	}

	public InterruptibleThread(ThreadGroup group, InterruptibleRunner<?> runner) {
		super(group, runner);
		this.runner = runner;
	}

	public InterruptibleThread(ThreadGroup group, InterruptibleRunner<?> runner, String name) {
		super(group, runner, name);
		this.runner = runner;
	}

	public InterruptibleThread(ThreadGroup group, InterruptibleRunner<?> runner, String name, long stackSize) {
		super(group, runner, name, stackSize);
		this.runner = runner;
	}
	
	@Override
	public void interrupt() {
		super.interrupt();
		if (runner != null) runner.interrupt();
	}

}
