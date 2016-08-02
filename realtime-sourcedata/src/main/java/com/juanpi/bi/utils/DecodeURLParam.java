package com.juanpi.bi.utils;

import org.apache.hadoop.hive.ql.exec.Description;

import java.util.ArrayList;
import java.util.List;

/**
 * 使用URLDecoder递归解码URL加密的参数
 *
 * @author zhuokun
 */

@Description(name = "DecodeURLParam", value = "_FUNC_(strUrl) - 递归解码URL加密的参数.  " +
        "\n _FUNC(array<String>) 针对数组的重载版本. ")
public class DecodeURLParam {

    /**
     * 使用URLDecoder递归解析URL加密的参数
     *
     * @param s, 待解码的URL参数值
     * @return 解码后的URL参数
     */

    public String evaluate(final String s) {
        if (s == null) {
            return null;
        }
        String res = StringUtils.recursiveDecode(s.trim());
        return res.trim();
    }

    /**
     * 使用URLDecoder递归解析URL加密的参数， 数组的重载版本
     *
     * @param s, 待解码的URL参数值
     * @return 解码后的URL参数
     */
    public List<String> evaluate(final List<String> s) {
        if (s == null) {
            return null;
        }
        List<String> result = new ArrayList<String>();
        for (String str : s) {
            if (str == null) {
                result.add("");
            } else {
                result.add(evaluate(str));
            }
        }
        return result;
    }


    public static void main(String[] argc) {

    }
}
