package com.ytfs.service.dao;

import java.util.ArrayList;
import java.util.List;

public class WriteObject {
    
    
    

    private String opName;
    private List<String> params;

    public void addParam(String param) {
        if (params == null) {
            params = new ArrayList();
        }
        params.add(param);
    }

    /**
     * @return the opName
     */
    public String getOpName() {
        return opName;
    }

    /**
     * @param opName the opName to set
     */
    public void setOpName(String opName) {
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
