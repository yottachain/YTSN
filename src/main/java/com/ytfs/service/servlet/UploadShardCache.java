package com.ytfs.service.servlet;

public class UploadShardCache {

    private byte[] VHF;//已上传的,未上传或失败的==null
    private int res;    //从存储节点返回的存储结果
    private int nodeid; //从存储节点返回节点ID 
    private int shardid;

    /**
     * @return the VHF
     */
    public byte[] getVHF() {
        return VHF;
    }

    /**
     * @param VHF the VHF to set
     */
    public void setVHF(byte[] VHF) {
        this.VHF = VHF;
    }

    /**
     * @return the res
     */
    public int getRes() {
        return res;
    }

    /**
     * @param res the res to set
     */
    public void setRes(int res) {
        this.res = res;
    }

    /**
     * @return the nodeid
     */
    public int getNodeid() {
        return nodeid;
    }

    /**
     * @param nodeid the nodeid to set
     */
    public void setNodeid(int nodeid) {
        this.nodeid = nodeid;
    }

    /**
     * @return the shardid
     */
    public int getShardid() {
        return shardid;
    }

    /**
     * @param shardid the shardid to set
     */
    public void setShardid(int shardid) {
        this.shardid = shardid;
    }

}
