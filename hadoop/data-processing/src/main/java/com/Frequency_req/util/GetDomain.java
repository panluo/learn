package util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetDomain {
	public static String getDomain(String url){
		if(url.isEmpty() || url.trim().equals("") || url == null){
			return "unknown";
		}
		String domain = url;
		Pattern p = Pattern.compile("(?<=//)((\\w)+\\.)+\\w+");
		Matcher matcher = p.matcher(url);  
		if(matcher.find()){
			domain = matcher.group();
			if(domain.startsWith("www."))
				domain = domain.substring(4);
		}
		return domain;
	}
}
