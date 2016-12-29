package com.juanpi.bi.mapred;

/**
 * Created by gongzi on 2016/12/29.
 */
public class CommonLogic {

    /**
     * 根据page_id 和 event_id 来判断页面层级
     * @param pageId
     * @param eventId
     * @return
     */
    public static String getPageLevelId(String pageLevelId, String pageId, String eventId) {
        String pageLvlId = pageLevelId;
        // 推荐点击为入口页(购物袋页、品牌页、商祥页底部)
        if("481".equals(eventId) || "10041".equals(eventId)){
            if("158".equals(pageId) || "167".equals(pageId) || "250".equals(pageId) || "26".equals(pageId)) {
                pageLvlId = "1";
            }
        } else if("10043".equals(eventId)){
            if("10084".equals(pageId) || "10085".equals(pageId)){
                pageLvlId = "5";
            }
        } else if("448".equals(eventId)){
            if("158".equals(pageId)){
                pageLvlId = "5";
            }
        }
        return pageLvlId;
    }

    public static String getVisitPath(String initStr, int pageLvlId,String pageLvl, String level1, String level2, String level3, String level4, String level5) {

        if(pageLvlId == 1){
            level1= pageLvl;
            level2 = initStr;
            level3 = initStr;
            level4 = initStr;
            level5 = initStr;
        } else if(pageLvlId == 2){
            level2= pageLvl;
            level3 = initStr;
            level4 = initStr;
            level5 = initStr;
        } else if(pageLvlId == 3){
            level3 = pageLvl;
            level4 = initStr;
            level5 = initStr;
        } else if(pageLvlId == 4){
            level4 = pageLvl;
            level5 = initStr;
        } else if(pageLvlId == 5){
            level5 = pageLvl;
        }

        String key = level1 + "\t" + level2 + "\t" + level3+ "\t" + level4 + "\t" + level5;
        return key;

    }

}
