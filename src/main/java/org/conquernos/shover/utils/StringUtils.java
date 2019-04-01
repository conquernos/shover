package org.conquernos.shover.utils;


public class StringUtils {

	public static String trim(String value, char[] chars) {
		char[] val = value.toCharArray();
		int len = val.length;
		int st = 0;

		while ((st < len) && isInCharacters(val[st], chars)) {
			st++;
		}
		while ((st < len) && isInCharacters(val[len - 1], chars)) {
			len--;
		}
		return ((st > 0) || (len < value.length())) ? value.substring(st, len) : value;
	}

	public static boolean isInCharacters(char c, char[] chars) {
		for (char dest : chars) if (dest == c) return true;
		return false;
	}

}
