package com.ytfs.service.servlet;

import io.yottachain.nodemgmt.core.vo.Node;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.types.ObjectId;

public class UploadBlockCache {

    private String userKey;
    private int shardcount;//该数据块分片数 
    private ObjectId VNU;
    private int errTimes = 0;
    private final Map<Integer, UploadShardCache> shardCaches = new ConcurrentHashMap();

    public UploadBlockCache(Node[] nodes, int shardcount, ObjectId VNU) {
        this.shardcount = shardcount;
        this.VNU = VNU;
    }

    public void addUploadShardCache(UploadShardCache cache) {
        shardCaches.put(cache.getShardid(), cache);
    }

    /**
     * @return the shardCaches
     */
    public Map<Integer, UploadShardCache> getShardCaches() {
        return shardCaches;
    }

    /**
     * @return the errTimes
     */
    public int getErrTimes() {
        return errTimes;
    }

    public int errInc() {
        return ++errTimes;
    }

    /**
     * @param errTimes the errTimes to set
     */
    public void setErrTimes(int errTimes) {
        this.errTimes = errTimes;
    }

    /**
     * @return the userid
     */
    public String getUserKey() {
        return userKey;
    }

    /**
     * @param userKey the userid to set
     */
    public void setUserKey(String userKey) {
        this.userKey = userKey;
    }

    /**
     * @return the shardcount
     */
    public int getShardcount() {
        return shardcount;
    }

    /**
     * @param shardcount the shardcount to set
     */
    public void setShardcount(int shardcount) {
        this.shardcount = shardcount;
    }

    /**
     * @return the VNU
     */
    public ObjectId getVNU() {
        return VNU;
    }

    /**
     * @param VNU the VNU to set
     */
    public void setVNU(ObjectId VNU) {
        this.VNU = VNU;
    }
}
