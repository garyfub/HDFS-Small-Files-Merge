package com.juanpi.bi.hiveUDF;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

public class GetShopId extends UDF{
	
	public String evaluate(final String url) {
		GetGoodsId gh=new GetGoodsId(); 
		if(url == null || url.trim().length()==0)
		{
			return null;
		}
		Pattern p = Pattern.compile("(shop_id=)([0-9]+)");
        Matcher m = p.matcher(url);
        if (m.find()) {
        	return gh.evaluate(m.group(2).toLowerCase());
        }
		return null;
	}
	
	public static void main(String[] argc){
		GetShopId gh=new GetShopId();
		List<String> urls = Arrays.asList
				(
						"http://m.juanpi.com/shop/skcdeal/7158611?skc=2",
						"http://m.juanpi.com/shop/skcdeal/7058668?skc=8",
						"http://m.juanpi.com/shop/skcdeal/7058668?skc=9",
						"http://m.juanpi.com/shop/skcdeal/7058668??????skc=10",
						"http://m.juanpi.com/brand/appbrand/1751592?shop_id=1949563",


						"http://m.juanpi.com/shop/7158611"
				);
		for(String url : urls)
		{
			String id = gh.evaluate(url);
			System.out.println("Id = " + id + ", " + url);
		}
	}
}