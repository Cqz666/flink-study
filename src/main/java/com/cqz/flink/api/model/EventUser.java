package com.cqz.flink.api.model;

import java.util.Date;

public class EventUser {
    /**
     * 用户标识
     */
    private String vid;

    /**
     * 第一次的渠道
     */
    private String firstChannel;

    /**
     * 时间
     */
    private Long firstTimestamp;

    /**
     * 日期
     */
    private String firstDatekey;

    /**
     * 更新时间
     */
    private Date updatetime;

    public EventUser(String vid, String firstChannel, Long firstTimestamp, String firstDatekey, Date updatetime) {
        this.vid = vid;
        this.firstChannel = firstChannel;
        this.firstTimestamp = firstTimestamp;
        this.firstDatekey = firstDatekey;
        this.updatetime = updatetime;
    }

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public String getFirstChannel() {
        return firstChannel;
    }

    public void setFirstChannel(String firstChannel) {
        this.firstChannel = firstChannel;
    }

    public Long getFirstTimestamp() {
        return firstTimestamp;
    }

    public void setFirstTimestamp(Long firstTimestamp) {
        this.firstTimestamp = firstTimestamp;
    }

    public String getFirstDatekey() {
        return firstDatekey;
    }

    public void setFirstDatekey(String firstDatekey) {
        this.firstDatekey = firstDatekey;
    }

    public Date getUpdatetime() {
        return updatetime;
    }

    public void setUpdatetime(Date updatetime) {
        this.updatetime = updatetime;
    }
}
