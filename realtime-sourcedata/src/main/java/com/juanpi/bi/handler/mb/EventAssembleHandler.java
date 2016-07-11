package com.juanpi.bi.handler.mb;


import com.alibaba.fastjson.JSON;
import com.juanpi.bi.bean.MbEvent;
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
        return JSON.parseObject(line,MbEvent.class);
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }
}
