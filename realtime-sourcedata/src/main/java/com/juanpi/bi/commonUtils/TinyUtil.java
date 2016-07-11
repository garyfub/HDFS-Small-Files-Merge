/**
 *
 * @(#)TinyUtil.java TODO版本信息  2014年10月20日
 * Copyright © 2010 - 2014 JuanPi.com 
 * All Rights Reserved   
 *
 **/
package com.juanpi.bi.commonUtils;

import net.sf.json.JSONObject;

/**
 * Description: TODO <br/>
 * 
 * <b>修改历史:</b> <br/>
 * 2014年10月20日 xiaopang Update Description <br/>
 */
public class TinyUtil {
    public static JSONObject getJsonBean(String msg) {
        return JSONObject.fromObject(msg);
    }
}
