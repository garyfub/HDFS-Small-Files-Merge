package com.juanpi.bi.commonUtils;


import com.juanpi.bi.bean.LoggerBean;

/**
 * Description: TODO <br/>
 * <p/>
 * <b>修改历史:</b> <br/>
 * 2014年9月26日 xiaopang Update Description <br/>
 */
public abstract class Parser {

    public LoggerBean loggerBean;

    public Parser(LoggerBean loggerBean) {
        this.loggerBean = loggerBean;
    }

    public abstract void parse();

    public LoggerBean getLoggerBean() {
        return loggerBean;
    }

    public void setLoggerBean(LoggerBean loggerBean) {
        this.loggerBean = loggerBean;
    }

}
