package com.juanpi.bi.bean;

import com.juanpi.bi.commonUtils.StringConstants;

import org.apache.commons.lang.StringUtils;

/**
 * Created by xiaopang on 1/4/15.
 */
public class MbEvent {

    private String ticks = StringUtils.EMPTY;
    private String session_id = StringUtils.EMPTY;                    
    private String activityname = StringUtils.EMPTY;                    
    private String starttime = StringUtils.EMPTY;                    
    private String endtime = StringUtils.EMPTY;                    
    private String result = StringUtils.EMPTY;                    
    private String uid = StringConstants.zero;
    private String extend_params = StringUtils.EMPTY;                    
    private String utm = StringUtils.EMPTY;                    
    private String source = StringUtils.EMPTY;
    
    private String starttime_origin = StringUtils.EMPTY;
    private String endtime_origin= StringUtils.EMPTY;
    
    private String app_name = StringUtils.EMPTY;
	private String app_version = StringUtils.EMPTY;
	private String os = StringUtils.EMPTY;
	private String pagename = StringUtils.EMPTY;
	private String page_extends_param = StringUtils.EMPTY;
	private String deviceid = StringUtils.EMPTY;
	private String pre_page = StringUtils.EMPTY;
	private String pre_extends_param = StringUtils.EMPTY;
	
	private String gj_page_names = StringUtils.EMPTY;
	private String gj_ext_params = StringUtils.EMPTY;
	
	private String jpid = StringUtils.EMPTY;
	private String ip = StringUtils.EMPTY;
	
	private String to_switch = StringUtils.EMPTY;
	private String cube_position = StringUtils.EMPTY;

	//add by chonglou on 2015-12-23 11:04
	private String location = StringUtils.EMPTY;
	private String c_label  = StringUtils.EMPTY;

	//服务器API返回得JSON字符串
	private String server_jsonstr;
	
	//add by yizhi on 2016-06-28
    private String c_server = StringUtils.EMPTY;
	
    public String getTicks() {
        return ticks;
    }

    public void setTicks(String ticks) {
        this.ticks = ticks;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getActivityname() {
        return activityname;
    }

    public void setActivityname(String activityname) {
        this.activityname = activityname;
    }

    public String getStarttime() {
        return starttime;
    }

    public void setStarttime(String starttime) {
        this.starttime = starttime;
    }

    public String getEndtime() {
        return endtime;
    }

    public void setEndtime(String endtime) {
        this.endtime = endtime;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getExtend_params() {
        return extend_params;
    }

    public void setExtend_params(String extend_params) {
        this.extend_params = extend_params;
    }

    public String getUtm() {
        return utm;
    }

    public void setUtm(String utm) {
        this.utm = utm;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
    

    public String getStarttime_origin() {
		return starttime_origin;
	}

	public void setStarttime_origin(String starttime_origin) {
		this.starttime_origin = starttime_origin;
	}

	public String getEndtime_origin() {
		return endtime_origin;
	}

	public void setEndtime_origin(String endtime_origin) {
		this.endtime_origin = endtime_origin;
	}

	public String getApp_name() {
		return app_name;
	}

	public void setApp_name(String app_name) {
		this.app_name = app_name;
	}

	public String getApp_version() {
		return app_version;
	}

	public void setApp_version(String app_version) {
		this.app_version = app_version;
	}

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getPagename() {
		return pagename;
	}

	public void setPagename(String pagename) {
		this.pagename = pagename;
	}

	public String getPage_extends_param() {
		return page_extends_param;
	}

	public void setPage_extends_param(String page_extends_param) {
		this.page_extends_param = page_extends_param;
	}

	public String getDeviceid() {
		return deviceid;
	}

	public void setDeviceid(String deviceid) {
		this.deviceid = deviceid;
	}

	public String getPre_page() {
		return pre_page;
	}

	public void setPre_page(String pre_page) {
		this.pre_page = pre_page;
	}

	public String getPre_extends_param() {
		return pre_extends_param;
	}

	public void setPre_extends_param(String pre_extends_param) {
		this.pre_extends_param = pre_extends_param;
	}

	public String getGj_page_names() {
		return gj_page_names;
	}

	public void setGj_page_names(String gj_page_names) {
		this.gj_page_names = gj_page_names;
	}

	public String getGj_ext_params() {
		return gj_ext_params;
	}

	public void setGj_ext_params(String gj_ext_params) {
		this.gj_ext_params = gj_ext_params;
	}

	public String getJpid() {
		return jpid;
	}

	public void setJpid(String jpid) {
		this.jpid = jpid;
	}
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getTo_switch() {
		return to_switch;
	}

	public void setTo_switch(String to_switch) {
		this.to_switch = to_switch;
	}

	public String getCube_position() {
		return cube_position;
	}

	public void setCube_position(String cube_position) {
		this.cube_position = cube_position;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getC_label() {
		return c_label;
	}

	public void setC_label(String c_label) {
		this.c_label = c_label;
	}

	public String getServer_jsonstr() {
		return server_jsonstr;
	}

	public void setServer_jsonstr(String server_jsonstr) {
		this.server_jsonstr = server_jsonstr;
	}

	public String getC_server() {
		return c_server;
	}

	public void setC_server(String c_server) {
		this.c_server = c_server;
	}

	public String format() {
        StringBuffer sb = new StringBuffer();

        String rst = sb.append(ticks).append(StringConstants.hiveFieldSeparator)
                .append(session_id).append(StringConstants.hiveFieldSeparator)
                .append(activityname).append(StringConstants.hiveFieldSeparator)
                .append(starttime).append(StringConstants.hiveFieldSeparator)
                .append(endtime).append(StringConstants.hiveFieldSeparator)
                .append(result).append(StringConstants.hiveFieldSeparator)
                .append(uid).append(StringConstants.hiveFieldSeparator)
                .append(extend_params).append(StringConstants.hiveFieldSeparator)
                .append(utm).append(StringConstants.hiveFieldSeparator)
                .append(source).append(StringConstants.hiveFieldSeparator)
                .append(starttime_origin).append(StringConstants.hiveFieldSeparator)
                .append(endtime_origin).append(StringConstants.hiveFieldSeparator)
			    .append(app_name).append(StringConstants.hiveFieldSeparator)
			    .append(app_version).append(StringConstants.hiveFieldSeparator)
			    .append(os).append(StringConstants.hiveFieldSeparator)
			    .append(pagename).append(StringConstants.hiveFieldSeparator)
			    .append(page_extends_param).append(StringConstants.hiveFieldSeparator)
			    .append(deviceid).append(StringConstants.hiveFieldSeparator)
			    .append(pre_page).append(StringConstants.hiveFieldSeparator)
			    .append(pre_extends_param).append(StringConstants.hiveFieldSeparator)
        		.append(gj_page_names).append(StringConstants.hiveFieldSeparator)
        		.append(gj_ext_params).append(StringConstants.hiveFieldSeparator)
        		.append(jpid).append(StringConstants.hiveFieldSeparator)
        		.append(ip).append(StringConstants.hiveFieldSeparator)
        		.append(to_switch).append(StringConstants.hiveFieldSeparator)
        		.append(cube_position).append(StringConstants.hiveFieldSeparator)
				.append(location).append(StringConstants.hiveFieldSeparator)
				.append(c_label).append(StringConstants.hiveFieldSeparator)
				.append(server_jsonstr).append(StringConstants.hiveFieldSeparator)
				.append(c_server).append(StringConstants.hiveFieldSeparator)
        		.toString();
        return rst.replace("\n", "\\n").replace("\r", "\\r");
    }

}
