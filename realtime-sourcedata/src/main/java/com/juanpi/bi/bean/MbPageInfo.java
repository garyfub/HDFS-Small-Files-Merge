//package com.juanpi.bi.bean;
//
//import com.juanpi.bi.commonUtils.StringConstants;
//import org.apache.commons.lang.StringUtils;
//
//
//public class MbPageInfo {
//
//    private String ticks = StringUtils.EMPTY;
//    private String session_id = StringUtils.EMPTY;
//    private String pagename = StringUtils.EMPTY;
//    private String starttime = StringUtils.EMPTY;
//    private String endtime = StringUtils.EMPTY;
//    private String pre_page = StringUtils.EMPTY;
//    private String uid = StringConstants.zero;
//    private String extend_params = StringUtils.EMPTY;
//    private String app_name = StringUtils.EMPTY;
//    private String app_version = StringUtils.EMPTY;
//    private String os_version = StringUtils.EMPTY;
//    private String os = StringUtils.EMPTY;
//    // utm 从ticks_history中取
//    private String utm = StringUtils.EMPTY;
//    private String source = StringUtils.EMPTY;
//
//    private String starttime_origin = StringUtils.EMPTY;
//    private String endtime_origin= StringUtils.EMPTY;
//
//    private String pre_extend_params = StringUtils.EMPTY;
//	private String wap_url = StringUtils.EMPTY;
//	private String wap_pre_url = StringUtils.EMPTY;
//	private String deviceid = StringUtils.EMPTY;
//
//	private String jpid = StringUtils.EMPTY;
//	private String ip = StringUtils.EMPTY;
//
//	private String to_switch = StringUtils.EMPTY;
//
//    //add by chonglou on 2015-12-23 11:04
//    private String location = StringUtils.EMPTY;
//    private String c_label  = StringUtils.EMPTY;
//
//    private String server_jsonstr = StringUtils.EMPTY;
//
//    //add by yizhi on 2016-06-28
//    private String c_server = StringUtils.EMPTY;
//
//    public String getTicks() {
//        return ticks;
//    }
//
//    public void setTicks(String ticks) {
//        this.ticks = ticks;
//    }
//
//    public String getSession_id() {
//        return session_id;
//    }
//
//    public void setSession_id(String session_id) {
//        this.session_id = session_id;
//    }
//
//    public String getPagename() {
//        return pagename;
//    }
//
//    public void setPagename(String pagename) {
//        this.pagename = pagename;
//    }
//
//    public String getStarttime() {
//        return starttime;
//    }
//
//    public void setStarttime(String starttime) {
//        this.starttime = starttime;
//    }
//
//    public String getEndtime() {
//        return endtime;
//    }
//
//    public void setEndtime(String endtime) {
//        this.endtime = endtime;
//    }
//
//    public String getPre_page() {
//        return pre_page;
//    }
//
//    public void setPre_page(String pre_page) {
//        this.pre_page = pre_page;
//    }
//
//    public String getUid() {
//        return uid;
//    }
//
//    public void setUid(String uid) {
//        this.uid = uid;
//    }
//
//    public String getExtend_params() {
//        return extend_params;
//    }
//
//    public void setExtend_params(String extend_params) {
//        this.extend_params = extend_params;
//    }
//
//    public String getApp_name() {
//        return app_name;
//    }
//
//    public void setApp_name(String app_name) {
//        this.app_name = app_name;
//    }
//
//    public String getApp_version() {
//        return app_version;
//    }
//
//    public void setApp_version(String app_version) {
//        this.app_version = app_version;
//    }
//
//    public String getOs_version() {
//        return os_version;
//    }
//
//    public void setOs_version(String os_version) {
//        this.os_version = os_version;
//    }
//
//    public String getOs() {
//        return os;
//    }
//
//    public void setOs(String os) {
//        this.os = os;
//    }
//
//    public String getUtm() {
//        return utm;
//    }
//
//    public void setUtm(String utm) {
//        this.utm = utm;
//    }
//
//    public String getSource() {
//        return source;
//    }
//
//    public void setSource(String source) {
//        this.source = source;
//    }
//
//    public String getStarttime_origin() {
//		return starttime_origin;
//	}
//
//	public void setStarttime_origin(String starttime_origin) {
//		this.starttime_origin = starttime_origin;
//	}
//
//	public String getEndtime_origin() {
//		return endtime_origin;
//	}
//
//	public void setEndtime_origin(String endtime_origin) {
//		this.endtime_origin = endtime_origin;
//	}
//
//	public String getPre_extend_params() {
//		return pre_extend_params;
//	}
//
//	public void setPre_extend_params(String pre_extend_params) {
//		this.pre_extend_params = pre_extend_params;
//	}
//
//	public String getWap_url() {
//		return wap_url;
//	}
//
//	public void setWap_url(String wap_url) {
//		this.wap_url = wap_url;
//	}
//
//	public String getWap_pre_url() {
//		return wap_pre_url;
//	}
//
//	public void setWap_pre_url(String wap_pre_url) {
//		this.wap_pre_url = wap_pre_url;
//	}
//
//	public String getDeviceid() {
//		return deviceid;
//	}
//
//	public void setDeviceid(String deviceid) {
//		this.deviceid = deviceid;
//	}
//
//	public String getJpid() {
//		return jpid;
//	}
//
//	public void setJpid(String jpid) {
//		this.jpid = jpid;
//	}
//
//	public String getIp() {
//		return ip;
//	}
//
//	public void setIp(String ip) {
//		this.ip = ip;
//	}
//
//	public String getTo_switch() {
//		return to_switch;
//	}
//
//	public void setTo_switch(String to_switch) {
//		this.to_switch = to_switch;
//	}
//
//    public String getLocation() {
//        return location;
//    }
//
//    public void setLocation(String location) {
//        this.location = location;
//    }
//
//    public String getC_label() {
//        return c_label;
//    }
//
//    public void setC_label(String c_label) {
//        this.c_label = c_label;
//    }
//
//    public String getServer_jsonstr() {
//		return server_jsonstr;
//	}
//
//	public void setServer_jsonstr(String server_jsonstr) {
//		this.server_jsonstr = server_jsonstr;
//	}
//
//    public String getC_server() {
//		return c_server;
//	}
//
//	public void setC_server(String c_server) {
//		this.c_server = c_server;
//	}
//
//	public String format() {
//        StringBuffer sb = new StringBuffer();
//
//        String rst = sb.append(ticks).append(StringConstants.hiveFieldSeparator)
//                .append(session_id).append(StringConstants.hiveFieldSeparator)
//                .append(pagename).append(StringConstants.hiveFieldSeparator)
//                .append(starttime).append(StringConstants.hiveFieldSeparator)
//                .append(endtime).append(StringConstants.hiveFieldSeparator)
//                .append(pre_page).append(StringConstants.hiveFieldSeparator)
//                .append(uid).append(StringConstants.hiveFieldSeparator)
//                .append(extend_params).append(StringConstants.hiveFieldSeparator)
//                .append(app_name).append(StringConstants.hiveFieldSeparator)
//                .append(app_version).append(StringConstants.hiveFieldSeparator)
//                .append(os_version).append(StringConstants.hiveFieldSeparator)
//                .append(os).append(StringConstants.hiveFieldSeparator)
//                .append(utm).append(StringConstants.hiveFieldSeparator)
//                .append(source).append(StringConstants.hiveFieldSeparator)
//                .append(starttime_origin).append(StringConstants.hiveFieldSeparator)
//                .append(endtime_origin).append(StringConstants.hiveFieldSeparator)
//		        .append(pre_extend_params).append(StringConstants.hiveFieldSeparator)
//		        .append(wap_url).append(StringConstants.hiveFieldSeparator)
//		        .append(wap_pre_url).append(StringConstants.hiveFieldSeparator)
//		        .append(deviceid).append(StringConstants.hiveFieldSeparator)
//		        .append(jpid).append(StringConstants.hiveFieldSeparator)
//		        .append(ip).append(StringConstants.hiveFieldSeparator)
//                .append(to_switch).append(StringConstants.hiveFieldSeparator)
//                .append(location).append(StringConstants.hiveFieldSeparator)
//                .append(c_label).append(StringConstants.hiveFieldSeparator)
//                .append(server_jsonstr).append(StringConstants.hiveFieldSeparator)
//                .append(c_server).append(StringConstants.hiveFieldSeparator)
//		        .toString();
//
//        return rst.replace("\n", "\\n").replace("\r", "\\r");
//    }
//}
//
