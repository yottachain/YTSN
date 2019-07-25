package com.ytfs.service.dao;

import com.ytfs.common.Function;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class WriteObject {

    private int opName;
    private List<byte[]> params;

    public void addParam(long param) {
        if (params == null) {
            params = new ArrayList();
        }
        params.add(Function.long2bytes(param));
    }

    public void addParam(int param) {
        if (params == null) {
            params = new ArrayList();
        }
        params.add(Function.int2bytes(param));
    }

    public void addParam(byte[] param) {
        if (params == null) {
            params = new ArrayList();
        }
        params.add(param);
    }

    public void addParam(String param) {
        if (params == null) {
            params = new ArrayList();
        }
        params.add(param.getBytes(Charset.forName("UTF-8")));
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
    public List<byte[]> getParams() {
        return params;
    }

    /**
     * @param params the params to set
     */
    public void setParams(List<byte[]> params) {
        this.params = params;
    }

}
