package com.ytfs.service.servlet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UploadObjectCache {

    private int userid;
    private long filesize;
    private long usedspace;
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

    /**
     * @return the filesize
     */
    public long getFilesize() {
        return filesize;
    }

    /**
     * @param filesize the filesize to set
     */
    public void setFilesize(long filesize) {
        this.filesize = filesize;
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

    /**
     * @return the usedspace
     */
    public long getUsedspace() {
        return usedspace;
    }

    /**
     * @param usedspace the usedspace to set
     */
    public void setUsedspace(long usedspace) {
        this.usedspace = usedspace;
    }

    public synchronized void addUsedspace(long addspace) {
        this.usedspace = this.usedspace + addspace;
    }
}
