package com.ytfs.service.dao;

import org.bson.Document;
import org.bson.types.Binary;

public class User {

    private int userID;
    private String username;
    private byte[] KUEp;    //用户公钥
    private long usedSpace;  //占用空间
    private long costPerCycle;//每周期总费用
    private long fileTotal;   //文件数
    private long spaceTotal;  //文件总量

    public User(int userid) {
        this.userID = userid;
    }

    public User(Document doc) {
        if (doc.containsKey("_id")) {
            this.userID = doc.getInteger("_id");
        }
        if (doc.containsKey("KUEp")) {
            this.KUEp = ((Binary) doc.get("KUEp")).getData();
        }
        if (doc.containsKey("usedSpace")) {
            this.usedSpace = doc.getLong("usedSpace");
        }
        if (doc.containsKey("costPerCycle")) {
            this.costPerCycle = doc.getLong("costPerCycle");
        }
        if (doc.containsKey("spaceTotal")) {
            this.spaceTotal = doc.getLong("spaceTotal");
        }
        if (doc.containsKey("fileTotal")) {
            this.fileTotal = doc.getLong("fileTotal");
        }
        if (doc.containsKey("username")) {
            this.username = doc.getString("username");
        }
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("_id", userID);
        doc.append("KUEp", new Binary(KUEp));
        doc.append("usedSpace", usedSpace);
        doc.append("costPerCycle", costPerCycle);
        doc.append("spaceTotal", spaceTotal);
        doc.append("fileTotal", fileTotal);
        doc.append("username", username);
        return doc;
    }

    /**
     * @return the userID
     */
    public int getUserID() {
        return userID;
    }

    /**
     * @param userID the userID to set
     */
    public void setUserID(int userID) {
        this.userID = userID;
    }

    /**
     * @return the KUEp
     */
    public byte[] getKUEp() {
        return KUEp;
    }

    /**
     * @param KUEp the KUEp to set
     */
    public void setKUEp(byte[] KUEp) {
        this.KUEp = KUEp;
    }

    /**
     * @return the usedSpace
     */
    public long getUsedSpace() {
        return usedSpace;
    }

    /**
     * @param usedSpace the usedSpace to set
     */
    public void setUsedSpace(long usedSpace) {
        this.usedSpace = usedSpace;
    }

    /**
     * @return the totalBaseCost
     */
    public long getCostPerCycle() {
        return costPerCycle;
    }

    /**
     * @param costPerCycle
     */
    public void setCostPerCycle(long costPerCycle) {
        this.costPerCycle = costPerCycle;
    }

    /**
     * @return the fileTotal
     */
    public long getFileTotal() {
        return fileTotal;
    }

    /**
     * @param fileTotal the fileTotal to set
     */
    public void setFileTotal(long fileTotal) {
        this.fileTotal = fileTotal;
    }

    /**
     * @return the spaceTotal
     */
    public long getSpaceTotal() {
        return spaceTotal;
    }

    /**
     * @param spaceTotal the spaceTotal to set
     */
    public void setSpaceTotal(long spaceTotal) {
        this.spaceTotal = spaceTotal;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

}
