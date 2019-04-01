package org.conquernos.shover.stats;


import java.util.concurrent.atomic.AtomicLong;

public class ShoverStats {

	private AtomicLong numberOfMessages = new AtomicLong(0);
	private AtomicLong numberOfCompletedMessages = new AtomicLong(0);


	public long getNumberOfMessages() {
		return numberOfMessages.get();
	}

	public long getNumberOfCompletedMessages() {
		return numberOfCompletedMessages.get();
	}

	public long addNumberOfMessages(long number) {
		return numberOfMessages.addAndGet(number);
	}

	public long addNumberOfCompletedMessages(long number) {
		return numberOfCompletedMessages.addAndGet(number);
	}

	@Override
	public String toString() {
		return "ShoverStats{" +
			"numberOfMessages=" + numberOfMessages +
			", numberOfCompletedMessages=" + numberOfCompletedMessages +
			'}';
	}

}
