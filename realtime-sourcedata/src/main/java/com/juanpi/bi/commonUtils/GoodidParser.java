package com.juanpi.bi.commonUtils;
import com.juanpi.bi.bean.LoggerBean;
import org.apache.commons.lang3.StringUtils;

public class GoodidParser extends Parser {

    public GoodidParser(LoggerBean loggerBean) {
        super(loggerBean);
    }

    @Override
    public void parse() {
         //得到LoggerBean，再解析
        String vstUrlStr = getLoggerBean().getUrl();
        if (StringUtils.isEmpty(vstUrlStr)) {
            return;
        }

        String dgoodid = getLoggerBean().getGoodid() ;

        getLoggerBean().setdGoodid(ParseUtil.decodeGoodid(dgoodid).trim());
        
    }

}

