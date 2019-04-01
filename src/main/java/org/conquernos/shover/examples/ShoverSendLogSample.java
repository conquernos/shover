package org.conquernos.shover.examples;


import org.conquernos.shover.Shover;
import org.conquernos.shover.ShoverMessage;
import org.conquernos.shover.exceptions.ShoverException;

public class ShoverSendLogSample {

	public static final Shover shover = Shover.getInstance();

	public static class MyLog extends ShoverMessage {
		public String svc;
		public String ssrl;
		public int depth;
		public String url;
		public String document;

		public MyLog(String svc, String ssrl, int depth, String url, String document) {
			this.svc = svc;
			this.ssrl = ssrl;
			this.depth = depth;
			this.url = url;
			this.document = document;
		}
	}

	public static void main(String[] args) throws ShoverException {
		for (int i=0; i<10; i++) {
//			shover.send(new MyLog("svc-" + i, "ssrl-" + i, i, "url-"+i, "doc-"+i));
			shover.send(new Object[]{"name-" + i, i, Float.valueOf(i + "." + i)});
		}
	}

}
