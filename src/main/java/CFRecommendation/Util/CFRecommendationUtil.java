package CFRecommendation.Util;

import java.util.ArrayList;
import java.util.List;

public class CFRecommendationUtil {
	

	public static String[] split(String str) {
		List<String> result = new ArrayList<String>();
		int pre = 0;
		int post = 0;
		int index = 0;
		int length = str.length();
		while (index < length) {
			// it doesn't begin with \",implementing that it's integer or
			// timestamp
			if (str.charAt(index) != '\"') {
				pre = index;
				while (index < length) {

					if (str.charAt(index) == ',') {
						post = index;
						index++;
						if (pre == post)
							result.add("");
						else
							result.add(str.substring(pre, post));
						break;
					}
					index++;
				}
			}
			// it begines with \",implementing that it's char
			else {
				index++;
				pre = index;
				while (index < length) {
					if (index == length - 1) {
						if (str.charAt(index) == '\"'
								&& str.charAt(index - 1) != '\\')
							result.add(str.substring(pre, length - 1));
						else
							result.add(str.substring(pre, length));
					} else if (str.charAt(index) == '\"'
							&& str.charAt(index - 1) != '\\'
							&& str.charAt(index + 1) == ',') {

						post = index;// "
						index++;// ,
						index++;

						result.add(str.substring(pre, post));
						break;
					}
					index++;
				}
			}
		}

		int listLength = result.size();
		String[] stringResult = new String[listLength];
		for (int i = 0; i < listLength; i++) {
			stringResult[i] = result.get(i).trim();
		}

		return stringResult;
	}
}
