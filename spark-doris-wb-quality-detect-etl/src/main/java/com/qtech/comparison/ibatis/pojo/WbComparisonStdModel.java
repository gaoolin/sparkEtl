package com.qtech.comparison.ibatis.pojo;

import java.io.Serializable;

/**
 * <p>
 *
 * </p>
 *
 * @author zhilin.gao
 * @since 2022-06-10
 */

public class WbComparisonStdModel implements Serializable {

	private static final long serialVersionUID = 1L;

	private String stdMcId;

	private String stdLineNo;

	private String stdPadX;

	private String stdPadY;

	private String stdLeadX;

	private String stdLeadY;

	private String leadThreshold;

	private String padThreshold;

	private String stdLeadDiff;

	private String stdPadDiff;

	private String stdWireLen;

	public String getStdMcId() {
		return stdMcId;
	}

	public void setStdMcId(String stdMcId) {
		this.stdMcId = stdMcId;
	}

	public String getStdLineNo() {
		return stdLineNo;
	}

	public void setStdLineNo(String stdLineNo) {
		this.stdLineNo = stdLineNo;
	}

	public String getStdPadX() {
		return stdPadX;
	}

	public void setStdPadX(String stdPadX) {
		this.stdPadX = stdPadX;
	}

	public String getStdPadY() {
		return stdPadY;
	}

	public void setStdPadY(String stdPadY) {
		this.stdPadY = stdPadY;
	}

	public String getStdLeadX() {
		return stdLeadX;
	}

	public void setStdLeadX(String stdLeadX) {
		this.stdLeadX = stdLeadX;
	}

	public String getStdLeadY() {
		return stdLeadY;
	}

	public void setStdLeadY(String stdLeadY) {
		this.stdLeadY = stdLeadY;
	}

	public String getLeadThreshold() {
		return leadThreshold;
	}

	public void setLeadThreshold(String leadThreshold) {
		this.leadThreshold = leadThreshold;
	}

	public String getPadThreshold() {
		return padThreshold;
	}

	public void setPadThreshold(String padThreshold) {
		this.padThreshold = padThreshold;
	}

	public String getStdLeadDiff() {
		return stdLeadDiff;
	}

	public void setStdLeadDiff(String stdLeadDiff) {
		this.stdLeadDiff = stdLeadDiff;
	}

	public String getStdPadDiff() {
		return stdPadDiff;
	}

	public void setStdPadDiff(String stdPadDiff) {
		this.stdPadDiff = stdPadDiff;
	}

	public String getStdWireLen() {
		return stdWireLen;
	}

	public void setStdWireLen(String stdWireLen) {
		this.stdWireLen = stdWireLen;
	}

	@Override
	public String toString() {
		return "StdMod{" +
				"stdMcId='" + stdMcId + '\'' +
				", stdLineNo='" + stdLineNo + '\'' +
				", stdPadX='" + stdPadX + '\'' +
				", stdPadY='" + stdPadY + '\'' +
				", stdLeadX='" + stdLeadX + '\'' +
				", stdLeadY='" + stdLeadY + '\'' +
				", leadThreshold='" + leadThreshold + '\'' +
				", padThreshold='" + padThreshold + '\'' +
				", stdLeadDiff='" + stdLeadDiff + '\'' +
				", stdPadDiff='" + stdPadDiff + '\'' +
				", stdWireLen='" + stdWireLen + '\'' +
				'}';
	}
}
