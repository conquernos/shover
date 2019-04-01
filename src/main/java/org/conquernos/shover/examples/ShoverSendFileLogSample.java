package org.conquernos.shover.examples;


import org.conquernos.shover.Shover;
import org.conquernos.shover.exceptions.ShoverException;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ShoverSendFileLogSample {

	public static final Shover shover = Shover.getInstance();

	public static void main(String[] args) throws ShoverException, IOException {
		String filePath = args[0];
		File file = new File(filePath);

		BufferedReader input = new BufferedReader(new FileReader(file));

		String line;
		while ((line = input.readLine()) != null) {
			String[] strs = line.split("\\|");
			Object[] values = new Object[strs.length];

			for (int i=0; i<strs.length; i++) {
				String str = strs[i];
				values[i] = NumberUtils.isNumber(str)? Integer.parseInt(str) : str;
			}

			shover.send(values);
		}

		System.out.printf("# of messages : %d\n", shover.getNumberOfMessages());
	}

}
