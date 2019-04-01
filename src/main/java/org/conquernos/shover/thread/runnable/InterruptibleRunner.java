package org.conquernos.shover.thread.runnable;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class InterruptibleRunner<V> implements Runnable, Serializable {

	private AtomicBoolean isInterrupted = new AtomicBoolean(false);

	private AtomicReference<V> reference;
	
	public InterruptibleRunner() {
		reference = new AtomicReference<>();
	}

	public InterruptibleRunner(V data) {
		this();
		setData(data);
	}

	public InterruptibleRunner(AtomicReference<V> reference) {
		this.reference = reference;
	}

	public final void setData(V data) {
		reference.set(data);
	}

	public final V getData() {
		return reference.get();
	}
	
	public final void interrupt() {
		isInterrupted.set(true);
	}

	protected boolean isInterrupted() {
		return isInterrupted.get();
	}

}
