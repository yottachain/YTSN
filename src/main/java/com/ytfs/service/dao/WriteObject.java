package com.ytfs.service.dao;

import io.yottachain.p2phost.utils.Base58;
import java.util.ArrayList;
import java.util.List;

public class WriteObject {

    private int opName;
    private List<String> params;

    public void addParam(byte[] param) {
        if (params == null) {
            params = new ArrayList();
        }
        params.add(Base58.encode(param));
    }

    public void addParam(String param) {
        if (params == null) {
            params = new ArrayList();
        }
        params.add(param);
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
    public List<String> getParams() {
        return params;
    }

    /**
     * @param params the params to set
     */
    public void setParams(List<String> params) {
        this.params = params;
    }

}
