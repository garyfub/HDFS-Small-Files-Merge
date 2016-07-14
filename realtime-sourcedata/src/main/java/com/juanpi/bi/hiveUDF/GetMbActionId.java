package com.juanpi.bi.hiveUDF;

import com.juanpi.bi.utils.IDSChecker;
import com.juanpi.bi.utils.IDSChecker.Page_Pattern;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;

/**
 * Created by gongzi on 2016/7/11.
 */
public class GetMbActionId extends UDF{

    public Integer evaluate(final String s) {
        IDSChecker.getInstance("MbActionID.properties");
        List<Page_Pattern> patterns = IDSChecker.patterns;
        if(patterns==null || patterns.isEmpty()) return -1;
        for (Page_Pattern pat : patterns) {
            if (pat.match(s.toLowerCase())) {
                return pat.id;
            }
        }
        return -1;
    }

    public Integer evaluate(String str1,String str2) {
        if(str1==null||str1.trim().length()==0)
            return -1;
        if(str2==null||str2.trim().length()==0)
            str2="";
        IDSChecker.getInstance("MbActionID.properties");
        List<Page_Pattern> patterns = IDSChecker.patterns;
        if(patterns==null || patterns.isEmpty()) return -1;
        for (Page_Pattern pat : patterns) {
            if (pat.match(str1.toLowerCase(),str2.toLowerCase())) {
                return pat.id;
            }
        }
        return -1;
    }


    public static void main(String[] argc){
        GetMbActionId gh=new GetMbActionId();
        System.out.println(gh.evaluate("Click_navigation","piall"));
    }

}
