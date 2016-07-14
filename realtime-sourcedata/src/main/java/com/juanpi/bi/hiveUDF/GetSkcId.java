package com.juanpi.bi.hiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gongzi on 2016/4/14.
 */
public class GetSkcId extends UDF {

    public String evaluate(String s) {
        String result = "";
        if (s == null) s = "";
        s = s.toLowerCase();
        Pattern pattern = Pattern.compile("(/shop/skcdeal)/(\\d+)\\?+skc=(\\d+)?");
        Matcher matcher = pattern.matcher(s);
        if (matcher.find()) {
            result = matcher.group(3);
        }
        return result;
    }

    public static void main(String[] args) {
        GetSkcId getSkcId = new GetSkcId();
        List<String> urls = Arrays.asList
        (
                "http://m.juanpi.com/shop/skcdeal/7158611?skc=2",
                "http://m.juanpi.com/shop/skcdeal/7058668?skc=8",
                "http://m.juanpi.com/shop/skcdeal/7058668?skc=9",
                "http://m.juanpi.com/shop/skcdeal/7058668??????skc=10",
                "http://m.juanpi.com/brand/appbrand/1751592?shop_id=1949563",
                "http://m.juanpi.com/shop/skcdeal/2254335?skc=3&qminkview=1&qmshareview=1",
                "http://m.juanpi.com/shop/7158611"
        );
        for(String url : urls)
        {
            String id = getSkcId.evaluate(url);
            System.out.println("Id = " + id + ", " + url);
        }
    }



}
