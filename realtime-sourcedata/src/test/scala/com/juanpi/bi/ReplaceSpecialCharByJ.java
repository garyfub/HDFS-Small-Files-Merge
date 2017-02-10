package com.juanpi.bi;

/**
 * Created by gongzi on 2017/2/10.
 */
public class ReplaceSpecialCharByJ {

    private String replaceChars(String str) {
        String res = str.replaceAll("(\0|\\s*|\r|\n)", "");
        return res;
    }

    /**
     * Intellij 中 java main的快捷键 psvm
     * @param args
     */
    public static void main(String[] args) {
        ReplaceSpecialCharByJ rep = new ReplaceSpecialCharByJ();
        String str = "品牌：秋壳	货号：4Q201	厚薄：常规\r\n衣门襟：其他	组合形式：单件	颜色：黑色 \n\r流行元素：其他	服装版型：直筒	衣长：常规款\r领型：圆领	风格：通勤	尺码：S,M,L,XS \n面料材质：聚酯纤维	年份季节：2017年春季";
        String res = rep.replaceChars(str);
        // 快捷键 sout
        System.out.println(res);
    }

}
