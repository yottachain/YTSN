package com.ytfs.service.servlet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UploadObjectCache {

    private int userid;
    private final List<Short> blockids = Collections.synchronizedList(new ArrayList());

    /**
     * @return the userid
     */
    public int getUserid() {
        return userid;
    }

    /**
     * @param userid the userid to set
     */
    public void setUserid(int userid) {
        this.userid = userid;
    }

    public void setBlockNums(short[] num) {
        for (short s : num) {
            this.blockids.add(s);
        }
    }

    public void setBlockNum(short num) {
        this.blockids.add(num);
    }

    public boolean exists(short num) {
        return this.blockids.contains(num);
    }
}
