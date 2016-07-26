package com.juanpi.bi.commonUtils;

import com.juanpi.bi.bean.LoggerBean;
import org.apache.commons.lang.StringUtils;

/**
 * Description: TODO <br/>
 * <p/>
 * <b>修改历史:</b> <br/>
 * 2014年9月26日 xiaopang Update Description <br/>
 */
public class SearchEngineAndKeywordParser extends com.juanpi.bi.commonUtils.Parser {

    public SearchEngineAndKeywordParser(LoggerBean loggerBean) {
        super(loggerBean);
    }

    @Override
    public void parse() {
        //得到RawLog，再解析
        String urlref = loggerBean.getUrlref();
        if(StringUtils.isEmpty(urlref))
            return;
        String[] strArr = SearchEnginKeywordUtil.getSearchEngineAndKeyword(urlref);
        if(strArr == null){
            return;
        }
        
        loggerBean.setSearchEngine(strArr[0]);
        loggerBean.setKeyword(strArr[1]);
    }

}

