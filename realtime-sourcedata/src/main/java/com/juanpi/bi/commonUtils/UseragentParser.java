package com.juanpi.bi.commonUtils;


import org.apache.commons.lang.StringUtils;
import com.juanpi.bi.bean.LoggerBean;

public class UseragentParser extends com.juanpi.bi.commonUtils.Parser {

    public UseragentParser(LoggerBean loggerBean) {
        super(loggerBean);
    }

    @Override
    public void parse() {
        //得到RawLog，再解析
        String userAgentStr = loggerBean.getHttp_user_agent();
        if (StringUtils.isEmpty(userAgentStr))
            return;

        ParseUtil.parseUserAgent(userAgentStr);

        loggerBean.setSpider(ParseUtil.userAgentMap.get("spider"));
        loggerBean.setBot(ParseUtil.userAgentMap.get("bot"));
        loggerBean.setIsSpider(ParseUtil.userAgentMap.get("isSpider"));
        loggerBean.setRenderingEngine(ParseUtil.userAgentMap.get("renderingEngine"));
        loggerBean.setBrowser(ParseUtil.userAgentMap.get("browser"));
        loggerBean.setBrowserType(ParseUtil.userAgentMap.get("browserType"));
        loggerBean.setIsMobileDevice(ParseUtil.userAgentMap.get("isMobileDevice"));
        loggerBean.setOperatingSystem(ParseUtil.userAgentMap.get("operatingSystem"));

        ParseUtil.userAgentMap.clear();

    }
}

