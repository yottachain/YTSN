package com.ytfs.service.dao;

import org.bson.Document;
import org.bson.types.Binary;

public class User {

    private int userID;
    private String eosName;
    private byte[] KUEp;
    private byte[] KUSp;
    private long usedSpace;
    private long totalBaseCost;

    public User(int userid) {
        this.userID = userid;
    }

    public User(Document doc) {
        if (doc.containsKey("_id")) {
            this.userID = doc.getInteger("_id");
        }
        if (doc.containsKey("eosName")) {
            this.eosName = doc.getString("eosName");
        }
        if (doc.containsKey("KUEp")) {
            this.KUEp = ((Binary) doc.get("KUEp")).getData();
        }
        if (doc.containsKey("KUSp")) {
            this.KUSp = ((Binary) doc.get("KUSp")).getData();
        }
        if (doc.containsKey("usedSpace")) {
            this.usedSpace = doc.getLong("usedSpace");
        }
        if (doc.containsKey("totalBaseCost")) {
            this.totalBaseCost = doc.getLong("totalBaseCost");
        }
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("_id", userID);
        doc.append("eosName", eosName);
        doc.append("KUEp", new Binary(KUEp));
        doc.append("KUSp", new Binary(KUSp));
        doc.append("usedSpace", usedSpace);
        doc.append("totalBaseCost", totalBaseCost);
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
     * @return the KUSp
     */
    public byte[] getKUSp() {
        return KUSp;
    }

    /**
     * @param KUSp the KUSp to set
     */
    public void setKUSp(byte[] KUSp) {
        this.KUSp = KUSp;
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
    public long getTotalBaseCost() {
        return totalBaseCost;
    }

    /**
     * @param totalBaseCost the totalBaseCost to set
     */
    public void setTotalBaseCost(long totalBaseCost) {
        this.totalBaseCost = totalBaseCost;
    }

    /**
     * @return the eosName
     */
    public String getEosName() {
        return eosName;
    }

    /**
     * @param eosName the eosName to set
     */
    public void setEosName(String eosName) {
        this.eosName = eosName;
    }
}
