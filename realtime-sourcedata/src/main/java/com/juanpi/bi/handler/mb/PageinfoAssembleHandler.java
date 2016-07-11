package com.juanpi.bi.handler.mb;


import com.alibaba.fastjson.JSONObject;
import com.juanpi.bi.handler.IHandler;

public class PageinfoAssembleHandler implements IHandler {

    private String line;

    @SuppressWarnings("unchecked")
    @Override
    public Object handle()
    {
        return JSONObject.parseObject(line);
//        return JSON.parseObject(line, MbPageInfo.class);
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

}
