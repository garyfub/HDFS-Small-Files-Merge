package com.juanpi.bi.bean;

import com.juanpi.bi.commonUtils.StringConstants;
import org.apache.commons.lang.StringUtils;

/**
 * @(#)Main.java TODO版本信息  2014年9月26日
 * <p/>
 * <p/>
 * /**
 * Description: TODO <br/>
 * <p/>
 * 2014年9月26日 xiaopang init <br/>
 * <b>修改历史:</b> <br/>
 */
public class LoggerBean {

    private String action_name = StringUtils.EMPTY;                    //"-1,,,",
    private String r = StringConstants.zero;                              //"744410",
    private String h = StringConstants.zero;                              //"14",
    private String m = StringConstants.zero;                              //"18",
    private String s = StringConstants.zero;                              //"29",
    private String url = StringUtils.EMPTY;                            //"http://www.juanpi.com/",

    private String utmid = StringConstants.zero;

    private String idsite = StringConstants.zero;

    private String rec = StringConstants.zero;

    private String ag = StringConstants.zero;

    private String pdf = StringConstants.zero;
    private String qt = StringConstants.zero;
    private String realp = StringConstants.zero;
    private String wma = StringConstants.zero;
    private String dir = StringConstants.zero;
    private String fla = StringConstants.zero;
    private String java = StringConstants.zero;
    private String gears = StringConstants.zero;

    private String cookie = StringUtils.EMPTY;

    private String dUtmid = StringConstants.zero;
    private String goodid = StringConstants.zero;
    private String dGoodid = StringConstants.zero;
    private String urlref = StringUtils.EMPTY;                         // "http://www.juanpi.com/",
    private String searchEngine = StringUtils.EMPTY;
    private String keyword = StringUtils.EMPTY;

    private String ul_id = StringUtils.EMPTY;                            //"1913f93f37cf6ee0",
    private String ul_idts = StringConstants.zero;                          //"1411712309",
    private String ul_idvc = StringConstants.zero;                          //"1",
    private String ul_refts = StringUtils.EMPTY;                         //"0",
    private String ul_viewts = StringUtils.EMPTY;                        //"1411712309",
    private String ul_idn = StringConstants.zero;
    private String ul_ref = StringUtils.EMPTY;                         //"1",

    private String res = StringUtils.EMPTY;                            //"1440x900",
    private String gt_ms = StringUtils.EMPTY;                          //"150",

    private String http_user_agent = StringUtils.EMPTY;
    private String spider = StringUtils.EMPTY;
    private String bot = StringUtils.EMPTY;
    private String isSpider = StringUtils.EMPTY;
    private String renderingEngine = StringUtils.EMPTY;
    private String browser = StringUtils.EMPTY;
    private String browserType = StringUtils.EMPTY;
    private String isMobileDevice = StringUtils.EMPTY;
    private String operatingSystem = StringUtils.EMPTY;

    private String ul_Qt = StringUtils.EMPTY;

    private String s_name = StringUtils.EMPTY;                         //"%E5%B0%8F%E9%BB%84%E5%BE%88%E9%97%B9%E5%BF%83",
    private String s_pic = StringUtils.EMPTY;                          //"/face/130218/mb5121ad904b975_326x325.jpg",
    private String s_sign = StringUtils.EMPTY;                         //"58c7a660e391fdc67622fe68effc211e",
    private String s_exp = StringUtils.EMPTY;                          //"14",
    private String s_uid = StringUtils.EMPTY;                          //"10nszi",
    private String sid = StringUtils.EMPTY;                            //"ijgem9lo0lhd8qb4q9r0l5rfj6",
    private String newPerson = StringUtils.EMPTY;                      //"1",

    private String utm = StringConstants.zero;
    private String dUtm = StringConstants.zero;

    private String isJump = StringConstants.falseStr;
    private String timestamp = StringConstants.zero;

    private String dateStr = StringUtils.EMPTY;
    private String sessionid = StringUtils.EMPTY;

    private String click_action_name = StringUtils.EMPTY;
    private String click_url = StringUtils.EMPTY;
    
    private String qm_ticks = StringUtils.EMPTY;
    private String qm_device_id = StringUtils.EMPTY;
    private String qm_system_ver = StringUtils.EMPTY;
    private String qm_app_ver = StringUtils.EMPTY;
    private String share_result = StringUtils.EMPTY;
    
    private String key_url_list = StringUtils.EMPTY;
    private String ip = StringUtils.EMPTY;
    private String jp_sid = StringUtils.EMPTY;
    private String jp_sid2 = StringUtils.EMPTY;
    private String jp_sid3 = StringUtils.EMPTY;
    private String jp_sid4 = StringUtils.EMPTY;
    private String jp_sid5 = StringUtils.EMPTY;
    private String jp_sid6 = StringUtils.EMPTY;
    private String jp_sid7 = StringUtils.EMPTY;
    private String qm_session_id = StringUtils.EMPTY;
    private String qm_jpid = StringUtils.EMPTY;
    
    private String jpk = StringConstants.zero;

    public String getAction_name() {
        return action_name;
    }

    public void setAction_name(String action_name) {
        this.action_name = action_name;
    }

    public String getR() {
        return r;
    }

    public void setR(String r) {
        this.r = r;
    }

    public String getH() {
        return h;
    }

    public void setH(String h) {
        this.h = h;
    }

    public String getM() {
        return m;
    }

    public void setM(String m) {
        this.m = m;
    }

    public String getS() {
        return s;
    }

    public void setS(String s) {
        this.s = s;
    }

    public String getIdsite() {
        return idsite;
    }

    public void setIdsite(String idsite) {
        this.idsite = idsite;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUtmid() {
        return utmid;
    }

    public void setUtmid(String utmid) {
        this.utmid = utmid;
    }

    public String getdUtmid() {
        return dUtmid;
    }

    public void setdUtmid(String dUtmid) {
        this.dUtmid = dUtmid;
    }

    public String getGoodid() {
        return goodid;
    }

    public void setGoodid(String goodid) {
        this.goodid = goodid;
    }

    public String getdGoodid() {
        return dGoodid;
    }

    public void setdGoodid(String dGoodid) {
        this.dGoodid = dGoodid;
    }

    public String getUrlref() {
        return urlref;
    }

    public void setUrlref(String urlref) {
        this.urlref = urlref;
    }

    public String getSearchEngine() {
        return searchEngine;
    }

    public void setSearchEngine(String searchEngine) {
        this.searchEngine = searchEngine;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getUl_id() {
        return ul_id;
    }

    public String getRec() {
        return rec;
    }

    public void setRec(String rec) {
        this.rec = rec;
    }

    public String getAg() {
        return ag;
    }

    public void setAg(String ag) {
        this.ag = ag;
    }

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    public String getPdf() {
        return pdf;
    }

    public void setPdf(String pdf) {
        this.pdf = pdf;
    }

    public String getQt() {
        return qt;
    }

    public void setQt(String qt) {
        this.qt = qt;
    }

    public void setUl_id(String ul_id) {
        this.ul_id = ul_id;
    }

    public String getUl_idts() {
        return ul_idts;
    }

    public void setUl_idts(String ul_idts) {
        this.ul_idts = ul_idts;
    }

    public String getUl_idvc() {
        return ul_idvc;
    }

    public void setUl_idvc(String ul_idvc) {
        this.ul_idvc = ul_idvc;
    }

    public String getUl_idn() {
        return ul_idn;
    }

    public void setUl_idn(String ul_idn) {
        this.ul_idn = ul_idn;
    }

    public String getUl_refts() {
        return ul_refts;
    }

    public void setUl_refts(String ul_refts) {
        this.ul_refts = ul_refts;
    }

    public String getUl_viewts() {
        return ul_viewts;
    }

    public void setUl_viewts(String ul_viewts) {
        this.ul_viewts = ul_viewts;
    }

    public String getUl_ref() {
        return ul_ref;
    }

    public void setUl_ref(String ul_ref) {
        this.ul_ref = ul_ref;
    }

    public String getRes() {
        return res;
    }

    public void setRes(String res) {
        this.res = res;
    }

    public String getGears() {
        return gears;
    }

    public void setGears(String gears) {
        this.gears = gears;
    }

    public String getJava() {
        return java;
    }

    public void setJava(String java) {
        this.java = java;
    }

    public String getFla() {
        return fla;
    }

    public void setFla(String fla) {
        this.fla = fla;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getWma() {
        return wma;
    }

    public void setWma(String wma) {
        this.wma = wma;
    }


    public String getRealp() {
        return realp;
    }

    public void setRealp(String realp) {
        this.realp = realp;
    }

    public String getGt_ms() {
        return gt_ms;
    }

    public void setGt_ms(String gt_ms) {
        this.gt_ms = gt_ms;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public String getSpider() {
        return spider;
    }

    public void setSpider(String spider) {
        this.spider = spider;
    }

    public String getBot() {
        return bot;
    }

    public void setBot(String bot) {
        this.bot = bot;
    }

    public String getIsSpider() {
        return isSpider;
    }

    public void setIsSpider(String isSpider) {
        this.isSpider = isSpider;
    }

    public String getRenderingEngine() {
        return renderingEngine;
    }

    public void setRenderingEngine(String renderingEngine) {
        this.renderingEngine = renderingEngine;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public String getBrowserType() {
        return browserType;
    }

    public void setBrowserType(String browserType) {
        this.browserType = browserType;
    }

    public String getIsMobileDevice() {
        return isMobileDevice;
    }

    public void setIsMobileDevice(String isMobileDevice) {
        this.isMobileDevice = isMobileDevice;
    }

    public String getOperatingSystem() {
        return operatingSystem;
    }

    public void setOperatingSystem(String operatingSystem) {
        this.operatingSystem = operatingSystem;
    }

    public String getUl_Qt() {
        return ul_Qt;
    }

    public void setUl_Qt(String ul_Qt) {
        this.ul_Qt = ul_Qt;
    }

    public String getS_name() {
        return s_name;
    }

    public void setS_name(String s_name) {
        this.s_name = s_name;
    }

    public String getS_pic() {
        return s_pic;
    }

    public void setS_pic(String s_pic) {
        this.s_pic = s_pic;
    }

    public String getS_sign() {
        return s_sign;
    }

    public void setS_sign(String s_sign) {
        this.s_sign = s_sign;
    }

    public String getS_exp() {
        return s_exp;
    }

    public void setS_exp(String s_exp) {
        this.s_exp = s_exp;
    }

    public String getS_uid() {
        return s_uid;
    }

    public void setS_uid(String s_uid) {
        this.s_uid = s_uid;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getNewPerson() {
        return newPerson;
    }

    public void setNewPerson(String newPerson) {
        this.newPerson = newPerson;
    }

    public String getUtm() {
        return utm;
    }

    public void setUtm(String utm) {
        this.utm = utm;
    }

    public String getdUtm() {
        return dUtm;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public void setdUtm(String dUtm) {
        this.dUtm = dUtm;
    }

    public String getDateStr() {
        return dateStr;
    }

    public void setDateStr(String dateStr) {
        this.dateStr = dateStr;
    }

    public String getIsJump() {
        return isJump;
    }

    public void setIsJump(String isJump) {
        this.isJump = isJump;
    }
    
    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public String getClick_action_name() {
		return click_action_name;
	}

	public void setClick_action_name(String click_action_name) {
		this.click_action_name = click_action_name;
	}

	public String getClick_url() {
		return click_url;
	}

	public void setClick_url(String click_url) {
		this.click_url = click_url;
	}

	public String getQm_ticks() {
		return qm_ticks;
	}

	public void setQm_ticks(String qm_ticks) {
		this.qm_ticks = qm_ticks;
	}

	public String getQm_device_id() {
		return qm_device_id;
	}

	public void setQm_device_id(String qm_device_id) {
		this.qm_device_id = qm_device_id;
	}

	public String getQm_system_ver() {
		return qm_system_ver;
	}

	public void setQm_system_ver(String qm_system_ver) {
		this.qm_system_ver = qm_system_ver;
	}

	public String getQm_app_ver() {
		return qm_app_ver;
	}

	public void setQm_app_ver(String qm_app_ver) {
		this.qm_app_ver = qm_app_ver;
	}

	public String getShare_result() {
		return share_result;
	}

	public void setShare_result(String share_result) {
		this.share_result = share_result;
	}

	public String getKey_url_list() {
		return key_url_list;
	}

	public void setKey_url_list(String key_url_list) {
		this.key_url_list = key_url_list;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getJp_sid() {
		return jp_sid;
	}

	public void setJp_sid(String jp_sid) {
		this.jp_sid = jp_sid;
	}

	public String getJp_sid2() {
		return jp_sid2;
	}

	public void setJp_sid2(String jp_sid2) {
		this.jp_sid2 = jp_sid2;
	}

	public String getJp_sid3() {
		return jp_sid3;
	}

	public void setJp_sid3(String jp_sid3) {
		this.jp_sid3 = jp_sid3;
	}

	public String getJp_sid4() {
		return jp_sid4;
	}

	public void setJp_sid4(String jp_sid4) {
		this.jp_sid4 = jp_sid4;
	}

	public String getJp_sid5() {
		return jp_sid5;
	}

	public void setJp_sid5(String jp_sid5) {
		this.jp_sid5 = jp_sid5;
	}

	public String getJp_sid6() {
		return jp_sid6;
	}

	public void setJp_sid6(String jp_sid6) {
		this.jp_sid6 = jp_sid6;
	}

	public String getJp_sid7() {
		return jp_sid7;
	}

	public void setJp_sid7(String jp_sid7) {
		this.jp_sid7 = jp_sid7;
	}


    public String getQm_session_id() {
        return qm_session_id;
    }

    public void setQm_session_id(String qm_session_id) {
        this.qm_session_id = qm_session_id;
    }

    public String getQm_jpid() {
        return qm_jpid;
    }

    public void setQm_jpid(String qm_jpid) {
        this.qm_jpid = qm_jpid;
    }

	public String getJpk() {
		return jpk;
	}

	public void setJpk(String jpk) {
		this.jpk = jpk;
	}

	public String format() {
        StringBuffer sb = new StringBuffer();

        String rst = sb.append(action_name).append(StringConstants.hiveFieldSeparator)
                        .append(r).append(StringConstants.hiveFieldSeparator)
                        .append(h).append(StringConstants.hiveFieldSeparator)
                        .append(m).append(StringConstants.hiveFieldSeparator)
                        .append(s).append(StringConstants.hiveFieldSeparator)
                        .append(url).append(StringConstants.hiveFieldSeparator)
                        .append(utmid).append(StringConstants.hiveFieldSeparator)
                        .append(dUtmid).append(StringConstants.hiveFieldSeparator)
                        .append(goodid).append(StringConstants.hiveFieldSeparator)
                        .append(dGoodid).append(StringConstants.hiveFieldSeparator)
                        .append(urlref).append(StringConstants.hiveFieldSeparator)
                        .append(searchEngine).append(StringConstants.hiveFieldSeparator)
                        .append(keyword).append(StringConstants.hiveFieldSeparator)
                        .append(ul_id).append(StringConstants.hiveFieldSeparator)
                        .append(ul_idts).append(StringConstants.hiveFieldSeparator)
                        .append(ul_idvc).append(StringConstants.hiveFieldSeparator)
                        .append(ul_ref).append(StringConstants.hiveFieldSeparator)
                        .append(ul_refts).append(StringConstants.hiveFieldSeparator)
                        .append(ul_viewts).append(StringConstants.hiveFieldSeparator)
                        .append(res).append(StringConstants.hiveFieldSeparator)
                        .append(gt_ms).append(StringConstants.hiveFieldSeparator)
                        .append(http_user_agent).append(StringConstants.hiveFieldSeparator)
                        .append(renderingEngine).append(StringConstants.hiveFieldSeparator)
                        .append(browser).append(StringConstants.hiveFieldSeparator)
                        .append(browserType).append(StringConstants.hiveFieldSeparator)
                        .append(isMobileDevice).append(StringConstants.hiveFieldSeparator)
                        .append(operatingSystem).append(StringConstants.hiveFieldSeparator)
                        .append(spider).append(StringConstants.hiveFieldSeparator)
                        .append(bot).append(StringConstants.hiveFieldSeparator)
                        .append(isSpider).append(StringConstants.hiveFieldSeparator)
                        .append(ul_Qt).append(StringConstants.hiveFieldSeparator)
                        .append(s_uid).append(StringConstants.hiveFieldSeparator)
                        .append(s_name).append(StringConstants.hiveFieldSeparator)
                        .append(s_pic).append(StringConstants.hiveFieldSeparator)
                        .append(s_sign).append(StringConstants.hiveFieldSeparator)
                        .append(s_exp).append(StringConstants.hiveFieldSeparator)
                        .append(sid).append(StringConstants.hiveFieldSeparator)
                        .append(newPerson).append(StringConstants.hiveFieldSeparator)
                        .append(utm).append(StringConstants.hiveFieldSeparator)
                        .append(dUtm).append(StringConstants.hiveFieldSeparator)
                        .append(timestamp).append(StringConstants.hiveFieldSeparator)
                        .append(dateStr).append(StringConstants.hiveFieldSeparator)
                        .append(isJump).append(StringConstants.hiveFieldSeparator)
                        .append(sessionid).append(StringConstants.hiveFieldSeparator)
                        .append(click_action_name).append(StringConstants.hiveFieldSeparator)
                        .append(click_url).append(StringConstants.hiveFieldSeparator)
				        .append(qm_ticks).append(StringConstants.hiveFieldSeparator)
				        .append(qm_device_id).append(StringConstants.hiveFieldSeparator)
				        .append(qm_system_ver).append(StringConstants.hiveFieldSeparator)
				        .append(qm_app_ver).append(StringConstants.hiveFieldSeparator)
				        .append(share_result).append(StringConstants.hiveFieldSeparator)
				        .append(key_url_list).append(StringConstants.hiveFieldSeparator)
				        .append(ip).append(StringConstants.hiveFieldSeparator)
				        .append(jp_sid).append(StringConstants.hiveFieldSeparator)
				        .append(jp_sid2).append(StringConstants.hiveFieldSeparator)
				        .append(jp_sid3).append(StringConstants.hiveFieldSeparator)
				        .append(jp_sid4).append(StringConstants.hiveFieldSeparator)
				        .append(jp_sid5).append(StringConstants.hiveFieldSeparator)
				        .append(jp_sid6).append(StringConstants.hiveFieldSeparator)
                        .append(jp_sid7).append(StringConstants.hiveFieldSeparator)
                        .append(qm_session_id).append(StringConstants.hiveFieldSeparator)
				        .append(qm_jpid).append(StringConstants.hiveFieldSeparator)
				        .append(jpk).toString();

        return rst.replace("\n","\\n").replace("\r","\\r");
    }

}
