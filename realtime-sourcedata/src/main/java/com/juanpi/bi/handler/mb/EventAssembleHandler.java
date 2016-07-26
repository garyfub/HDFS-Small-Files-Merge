package com.juanpi.bi.handler.mb;


import com.alibaba.fastjson.JSONObject;
import com.juanpi.bi.handler.IHandler;

/**
 * Description: TODO <br/>
 * <p/>
 * <b>修改历史:</b> <br/>
 * Jan 5, 2015 xiaopang init Description <br/>
 */
public class EventAssembleHandler implements IHandler {

    private String line;

    @SuppressWarnings("unchecked")
    @Override
    public Object handle() {
        return JSONObject.parseObject(line);
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }
}
