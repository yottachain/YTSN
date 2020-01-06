package com.ytfs.service.dao;

import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.types.Binary;

public class User {

    private int userID;
    private byte[][] KUEp;    //用户公钥

    private long usedspace;  //文件去重后占用空间总和
    private long spaceTotal;  //文件实际长度总和
    private long fileTotal;   //文件数

    private String username;
    private long costPerCycle;//每周期总费用
    private long nextCycle;
    private String relationship;

    public User(int userid) {
        this.userID = userid;
    }

    public User(Document doc) {
        if (doc.containsKey("_id")) {
            this.userID = doc.getInteger("_id");
        }
        if (doc.get("KUEp") != null) {
            List ls = (List) doc.get("KUEp");
            this.KUEp = new byte[ls.size()][];
            int index = 0;
            for (Object obj : ls) {
                this.KUEp[index++] = ((Binary) obj).getData();
            }
        }
        if (doc.containsKey("usedspace")) {
            this.usedspace = doc.getLong("usedspace");
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
        if (doc.containsKey("costPerCycle")) {
            this.costPerCycle = doc.getLong("costPerCycle");
        }
        if (doc.containsKey("nextCycle")) {
            this.nextCycle = doc.getLong("nextCycle");
        }
        if (doc.containsKey("relationship")) {
            this.relationship = doc.getString("relationship");
        }
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("_id", userID);
        List<Binary> kueps = new ArrayList();
        if (KUEp != null) {
            for (byte[] bs : KUEp) {
                kueps.add(new Binary(bs));
            }
        }
        doc.append("KUEp", kueps);
        doc.append("usedspace", usedspace);
        doc.append("costPerCycle", costPerCycle);
        doc.append("spaceTotal", spaceTotal);
        doc.append("fileTotal", fileTotal);
        doc.append("username", username);
        doc.append("nextCycle", nextCycle);
        doc.append("relationship", relationship);
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
    public byte[][] getKUEp() {
        return KUEp;
    }

    /**
     * @param KUEp the KUEp to set
     */
    public void setKUEp(byte[][] KUEp) {
        this.KUEp = KUEp;
    }

    /**
     * @return the usedSpace
     */
    public long getUsedspace() {
        return usedspace;
    }

    /**
     * @param usedSpace the usedSpace to set
     */
    public void setUsedspace(long usedSpace) {
        this.usedspace = usedSpace;
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

    /**
     * @return the nextCycle
     */
    public long getNextCycle() {
        return nextCycle;
    }

    /**
     * @param nextCycle the nextCycle to set
     */
    public void setNextCycle(long nextCycle) {
        this.nextCycle = nextCycle;
    }

    /**
     * @return the relationship
     */
    public String getRelationship() {
        return relationship;
    }

    /**
     * @param relationship the relationship to set
     */
    public void setRelationship(String relationship) {
        this.relationship = relationship;
    }

}
