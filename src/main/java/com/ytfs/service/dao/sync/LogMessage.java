package com.ytfs.service.dao.sync;

import com.ytfs.common.SerializationUtil;

public class LogMessage {

    private int opName;
    private byte[] params;

    public LogMessage() {
    }

    public LogMessage(int opName, byte[] params) {
        this.opName = opName;
        this.params = params;
    }

    public LogMessage(int opName, Object params) {
        this.opName = opName;
        this.params = SerializationUtil.serializeNoID(params);
    }

    /**
     * @return the opName
     */
    public int getOpName() {
        return opName;
    }

    /**
     * @param opName the opName to set
     */
    public void setOpName(int opName) {
        this.opName = opName;
    }

    /**
     * @return the params
     */
    public byte[] getParams() {
        return params;
    }

    /**
     * @param params the params to set
     */
    public void setParams(byte[] params) {
        this.params = params;
    }

    public byte[] toByte() {
        return SerializationUtil.serializeNoID(this);
    }
}
