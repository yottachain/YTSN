package com.ytfs.service.dao.sync;

import com.ytfs.service.dao.ObjectMeta;

public class ObjectMetaLog {

    private byte[] id;
    private long length;
    private String VNU;
    private long usedspace;
    private int NLINK;
    private byte[][] blockList;

    public ObjectMetaLog() {
    }

    public ObjectMetaLog(ObjectMeta meta) {
        //this.id = meta.getId();
        this.length = meta.getLength();
        this.VNU = meta.getVNU().toHexString();
        this.usedspace = meta.getUsedspace();
        this.NLINK = meta.getNLINK();
        this.blockList = meta.getBlockList();
    }

    /**
     * @return the id
     */
    public byte[] getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(byte[] id) {
        this.id = id;
    }

    /**
     * @return the length
     */
    public long getLength() {
        return length;
    }

    /**
     * @param length the length to set
     */
    public void setLength(long length) {
        this.length = length;
    }

    /**
     * @return the VNU
     */
    public String getVNU() {
        return VNU;
    }

    /**
     * @param VNU the VNU to set
     */
    public void setVNU(String VNU) {
        this.VNU = VNU;
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

    /**
     * @return the NLINK
     */
    public int getNLINK() {
        return NLINK;
    }

    /**
     * @param NLINK the NLINK to set
     */
    public void setNLINK(int NLINK) {
        this.NLINK = NLINK;
    }

    /**
     * @return the blockList
     */
    public byte[][] getBlockList() {
        return blockList;
    }

    /**
     * @param blockList the blockList to set
     */
    public void setBlockList(byte[][] blockList) {
        this.blockList = blockList;
    }

}
