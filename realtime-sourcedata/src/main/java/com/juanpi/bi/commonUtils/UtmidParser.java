package com.juanpi.bi.commonUtils;



import com.juanpi.bi.bean.LoggerBean;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Description: TODO <br/>
 * <p/>
 * <b>修改历史:</b> <br/>
 * 2014年9月26日 xiaopang Update Description <br/>
 */
public class UtmidParser extends com.juanpi.bi.commonUtils.Parser {

    public UtmidParser(LoggerBean loggerBean) {
        super(loggerBean);
    }

    @Override
    public void parse() {
        // 解析utm
         parseUtm();
        // 解析utmid
        parseUtmid();
    }

    private void parseUtm() {
        String utm = this.getLoggerBean().getUtm();
        StringBuilder sb = new StringBuilder();
        String[] utms = null;
        if (!StringUtils.isBlank(utm)) {
            utms = utm.split("\\.");
        } else {
            return;
        }

        for (int i = 0; i < utms.length; i++) {
            sb = sb.append(ParseUtil.decodeUidUtmid(utms[i])).append(".");
        }
        String dUtm = sb.toString();
        if(dUtm.isEmpty()) {
            dUtm = "0";
        }
        if (".".equals(dUtm.charAt(dUtm.length() - 1))) {
            dUtm = dUtm.substring(0, dUtm.length() - 1);
        }
        this.getLoggerBean().setdUtm(dUtm);
    }

    private void parseUtmid() {
        // 得到LoggerBean，再解析
        String url = this.getLoggerBean().getUrl();
        String utm = StringConstants.zero;
        if ((StringUtils.isEmpty(url) || (!(url.contains("utm="))))) {
            return;
        }
        Matcher matcher = Pattern.compile("utm=([\\d\\.]*)").matcher(url);
        if (matcher.find()) {
            utm = matcher.group(1);
        }
        if (StringUtils.isBlank(utm)){
            utm = StringConstants.zero;
        }
        this.getLoggerBean().setUtmid(utm);
        this.getLoggerBean().setdUtmid(utm);
    }

}
