package com.juanpi.bi.hiveUDF;

import com.juanpi.bi.utils.PageTypeUtils;
import org.apache.hadoop.hive.ql.exec.Description;

import java.util.Arrays;
import java.util.List;

/**
 * 获取页面类型，类型与数据库dim_page_type 中定义的Page_ID 一致，特殊返回值介绍：-1表示站内未知页，-999表示站外页，非法URL或空URL时返回null。
 * 用法：getPageid(String url)
 */
@Description(name = "GetPageID", value = "_FUNC_(string url1) - Return the page id of  url, can batch used.")
public class GetPageID // extends UDF
{
	
	/**
	 * 获取指定url 对应的一级页面类型
	 * @param s
	 * @return -1表示内部未知页，-999表示外部页
	 */
	public Integer evaluate(final String s) {
        return PageTypeUtils.getPageID(s);
    }
	
	public static void main(String[] argc){
		GetPageID gh=new GetPageID();
		{
            List<String> urls = Arrays.asList
            (
                    "http://m.juanpi.com/shop/skcdeal/2254335?skc=3&qminkview=1&qmshareview=1",
                    "http://m.juanpi.com/act/skcbrand?mobile=1&qminkview=1",
                    "http://m.juanpi.com/act/jxyg418?mobile=1&qminkview=1&qmshareview=1&from=singlemessage&isappinstalled=1",
                    "http://m.juanpi.com/act/sub_nsyg418?mobile=1&qminkview=1&qmshareview=1?qmshareview=1&from=singlemessage&isappinstalled=1",
                    "http://m.juanpi.com/act/skcbrand?mobile=1&qminkview=1",
					"http://m.juanpi.com/brand/appbrand/1751592?shop_id=1949563&amp;mobile=1&qminkview=1",
					"http://m.juanpi.com/zhuanti/qqshg",
					"http://m.juanpi.com/zhuanti/qqjzjj",
					"http://m.juanpi.com/zhuanti/qqcfcy",
					"http://m.juanpi.com/zhuanti/qqrybh",
					"http://m.juanpi.com/zhuanti/qqjfby",

					"http://m.juanpi.com/brand/appbrand/1751592?shop_id=1949563&mobile=1&qminkview=1"
                    );
            int pingtuan_id = 0;
			for(String url : urls)
			{
				pingtuan_id = gh.evaluate(url);
				System.out.println("page_id = " + pingtuan_id + ", " + url);
			}
		}
	}
}
