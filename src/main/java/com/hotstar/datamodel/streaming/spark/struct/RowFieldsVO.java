package com.hotstar.datamodel.streaming.spark.struct;

public class RowFieldsVO {
	
	String timestamp;
	String userid;
	String platform;
	String contentid;
	String error;
	String location;
	public RowFieldsVO(String timestamp, String userid, String platform, String contentid, String error,
			String location) {
		super();
		this.timestamp = timestamp;
		this.userid = userid;
		this.platform = platform;
		this.contentid = contentid;
		this.error = error;
		this.location = location;
	}
	
	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getContentid() {
		return contentid;
	}
	public void setContentid(String contentid) {
		this.contentid = contentid;
	}
	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	 
	
}
