//package com.juanpi.bi.handler;
//
//import com.juanpi.bi.bean.LoggerBean;
//import com.juanpi.bi.commonUtils.*;
//import com.juanpi.bi.commonUtils.DateUtil;
//import com.juanpi.bi.commonUtils.StringConstants;
//import net.sf.json.JSONObject;
//import org.apache.commons.lang3.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
///**
// * Description: TODO <br/>
// * <p/>
// * <b>修改历史:</b> <br/>
// * 2014年9月26日 xiaopang Update Description <br/>
// */
//@SuppressWarnings("unused")
//public class AssembleHandler implements IHandler {
//
//	private String line;
//	private Parser seakParser;
//	private Parser useragentParser;
//	private Parser goodidParser;
//	private Parser utmidParser;
//
//	private final String GoodidSparator = "/deal/";
//
//	private static final Logger LOG = LoggerFactory.getLogger(com.juanpi.bi.handler.AssembleHandler.class);
//	public AssembleHandler(String line) {
//		this.line = line;
//		seakParser = new SearchEngineAndKeywordParser(null);
//		useragentParser = new UseragentParser(null);
//		goodidParser = new GoodidParser(null);
//		utmidParser = new UtmidParser(null);
//	}
//
//	@Override
//	public Object handle() {
//		LoggerBean loggerBean = new LoggerBean();
//		JSONObject jb = JSONObject.fromObject(line);
//
//		if (jb.has("ua")) {
//			loggerBean.setHttp_user_agent(jb.getString("ua"));
//		}
//		if (jb.has("_Qt")) {
//			loggerBean.setUl_Qt(jb.getString("_Qt"));
//		}
//		if (jb.has("s_uid")) {
//			loggerBean.setS_uid(jb.getString("s_uid"));
//		}
//		if (jb.has("s_name")) {
//			loggerBean.setS_name(jb.getString("s_name"));
//		}
//		if (jb.has("s_pic")) {
//			loggerBean.setS_pic(jb.getString("s_pic"));
//		}
//		if (jb.has("s_sign")) {
//			loggerBean.setS_sign(jb.getString("s_sign"));
//		}
//		if (jb.has("s_exp")) {
//			loggerBean.setS_exp(jb.getString("s_exp"));
//		}
//		if (jb.has("sid")) {
//			loggerBean.setSid(jb.getString("sid"));
//		}
//		if (jb.has("newPerson")) {
//			loggerBean.setNewPerson(jb.getString("newPerson"));
//		}
//		if (jb.has("utm")) {
//			loggerBean.setUtm(jb.getString("utm"));
//		}
//		if (jb.has("timestamp")) {
//			String timestamp = jb.getString("timestamp");
//			loggerBean.setTimestamp(timestamp);
//
//			DateUtil.setTime((Long.parseLong(timestamp)));
//			loggerBean.setDateStr(DateUtil.getDateString());
//		}
//		if (jb.has("action_name")) {
//			loggerBean.setAction_name(jb.getString("action_name"));
//		}
//		if (jb.has("idsite")) {
//			loggerBean.setIdsite(jb.getString("idsite"));
//		}
//		if (jb.has("rec")) {
//			loggerBean.setRec(jb.getString("rec"));
//		}
//		if (jb.has("r")) {
//			loggerBean.setR(jb.getString("r"));
//		}
//		if (jb.has("h")) {
//			loggerBean.setH(jb.getString("h"));
//		}
//		if (jb.has("m")) {
//			loggerBean.setM(jb.getString("m"));
//		}
//		if (jb.has("s")) {
//			loggerBean.setS(jb.getString("s"));
//		}
//		if (jb.has("url")) {
//			String url = jb.getString("url");
//			String goodid = StringUtils.EMPTY;
//			loggerBean.setUrl(url);
//			// 获取goodid
//			Pattern pattern = Pattern.compile("deal/(\\d+)");
//			Matcher matcher = pattern.matcher(url);
//			if (matcher.find()) {
//				goodid = matcher.group(1);
//				loggerBean.setGoodid(goodid);
//			}
//
//			// 解析isJump
//			if (url.toLowerCase().indexOf("click/?id=") >= 0) {
//				loggerBean.setIsJump(StringConstants.trueStr);
//			}
//		}
//
//		if (jb.has("urlref")) {
//			loggerBean.setUrlref(jb.getString("urlref"));
//		}
//		if (jb.has("_id")) {
//			loggerBean.setUl_id(jb.getString("_id"));
//		}
//		if (jb.has("_idts")) {
//			loggerBean.setUl_idts(jb.getString("_idts"));
//		}
//		if (jb.has("_idvc")) {
//			loggerBean.setUl_idvc(jb.getString("_idvc"));
//		}
//		if (jb.has("_idn")) {
//			loggerBean.setUl_idn(jb.getString("_idn"));
//		}
//		if (jb.has("_refts")) {
//			loggerBean.setUl_refts(jb.getString("_refts"));
//		}
//		if (jb.has("_viewts")) {
//			loggerBean.setUl_viewts(jb.getString("_viewts"));
//		}
//		if (jb.has("_ref")) {
//			loggerBean.setUl_ref(jb.getString("_ref"));
//		}
//		if (jb.has("pdf")) {
//			loggerBean.setPdf(jb.getString("pdf"));
//		}
//		if (jb.has("qt")) {
//			loggerBean.setQt(jb.getString("qt"));
//		}
//		if (jb.has("realp")) {
//			loggerBean.setRealp(jb.getString("realp"));
//		}
//		if (jb.has("wma")) {
//			loggerBean.setWma(jb.getString("wma"));
//		}
//		if (jb.has("dir")) {
//			loggerBean.setDir(jb.getString("dir"));
//		}
//		if (jb.has("fla")) {
//			loggerBean.setFla(jb.getString("fla"));
//		}
//		if (jb.has("java")) {
//			loggerBean.setJava(jb.getString("java"));
//		}
//		if (jb.has("gears")) {
//			loggerBean.setGears(jb.getString("gears"));
//		}
//		if (jb.has("ag")) {
//			loggerBean.setAg(jb.getString("ag"));
//		}
//		if (jb.has("cookie")) {
//			loggerBean.setCookie(jb.getString("cookie"));
//		}
//		if (jb.has("res")) {
//			loggerBean.setRes(jb.getString("res"));
//		}
//		if (jb.has("gt_ms")) {
//			loggerBean.setGt_ms(jb.getString("gt_ms"));
//		}
//		if (jb.has("sessionid")) {
//			loggerBean.setSessionid(jb.getString("sessionid"));
//		}
//
//		if (jb.has("click_action_name")) {
//			loggerBean.setClick_action_name(jb.getString("click_action_name"));
//		}
//		if (jb.has("click_url")) {
//			loggerBean.setClick_url(jb.getString("click_url"));
//		}
//
//		if (jb.has("qm_ticks")) {
//			loggerBean.setQm_ticks(jb.getString("qm_ticks"));
//		}
//		if (jb.has("qm_device_id")) {
//			loggerBean.setQm_device_id(jb.getString("qm_device_id"));
//		}
//		if (jb.has("qm_system_ver")) {
//			loggerBean.setQm_system_ver(jb.getString("qm_system_ver"));
//		}
//		if (jb.has("qm_app_ver")) {
//			loggerBean.setQm_app_ver(jb.getString("qm_app_ver"));
//		}
//		if (jb.has("share_result")) {
//			loggerBean.setShare_result(jb.getString("share_result"));
//		}
//
//		if (jb.has("key_url_list")) {
//			loggerBean.setKey_url_list(jb.getString("key_url_list"));
//		}
//		if (jb.has("ip")) {
//			loggerBean.setIp(jb.getString("ip"));
//		}
//		if (jb.has("jp_sid")) {
//			loggerBean.setJp_sid(jb.getString("jp_sid"));
//		}
//		if (jb.has("jp_sid2")) {
//			loggerBean.setJp_sid2(jb.getString("jp_sid2"));
//		}
//		if (jb.has("jp_sid3")) {
//			loggerBean.setJp_sid3(jb.getString("jp_sid3"));
//		}
//		if (jb.has("jp_sid4")) {
//			loggerBean.setJp_sid4(jb.getString("jp_sid4"));
//		}
//		if (jb.has("jp_sid5")) {
//			loggerBean.setJp_sid5(jb.getString("jp_sid5"));
//		}
//		if (jb.has("jp_sid6")) {
//			loggerBean.setJp_sid6(jb.getString("jp_sid6"));
//		}
//		if (jb.has("jp_sid7")) {
//			loggerBean.setJp_sid7(jb.getString("jp_sid7"));
//		}
//		if (jb.has("qm_session_id")) {
//			loggerBean.setQm_session_id(jb.getString("qm_session_id"));
//		}
//		if (jb.has("qm_jpid")) {
//			loggerBean.setQm_jpid(jb.getString("qm_jpid"));
//		}
//		if (jb.has("jpk")) {
//			loggerBean.setJpk(jb.getString("jpk"));
//		}
//
//		// 解析SearchEngine And Keyword
//		seakParser.setLoggerBean(loggerBean);
//		seakParser.parse();
//		// 解析useragent
//		useragentParser.setLoggerBean(loggerBean);
//		useragentParser.parse();
//		// goodid 解析
//		goodidParser.setLoggerBean(loggerBean);
//		goodidParser.parse();
//		// utmid解析
//		utmidParser.setLoggerBean(loggerBean);
//		utmidParser.parse();
//
//		return loggerBean;
//	}
//
//	public String getLine() {
//		return line;
//	}
//
//	public void setLine(String line) {
//		this.line = line;
//	}
//
//	public Parser getUtmidParser() {
//		return utmidParser;
//	}
//
//	public void setUtmidParser(Parser utmidParser) {
//		this.utmidParser = utmidParser;
//	}
//
//	public Parser getGoodidParser() {
//		return goodidParser;
//	}
//
//	public void setGoodidParser(Parser goodidParser) {
//		this.goodidParser = goodidParser;
//	}
//
//	public Parser getUseragentParser() {
//		return useragentParser;
//	}
//
//	public void setUseragentParser(Parser useragentParser) {
//		this.useragentParser = useragentParser;
//	}
//
//	public Parser getSeakParser() {
//		return seakParser;
//	}
//
//	public void setSeakParser(Parser seakParser) {
//		this.seakParser = seakParser;
//	}
//
//}
