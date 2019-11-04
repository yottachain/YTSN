package com.ytfs.service.dao;

import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class ObjectMeta {

    private int userID;
    private byte[] VHW;
    private ObjectId VNU;
    private int NLINK;
    private long length;
    private long usedspace;
    private byte[][] blockList;

    public ObjectMeta(int userID, byte[] VHW) {
        this.userID = userID;
        this.VHW = VHW;
    }

    public ObjectMeta(int userID, Document doc) {
        this.userID = userID;
        fill(doc);
    }

    public final void fill(Document doc) {
        this.VHW = ((Binary) doc.get("_id")).getData();
        if (doc.containsKey("VNU")) {
            this.VNU = doc.getObjectId("VNU");
        }
        if (doc.containsKey("NLINK")) {
            this.NLINK = doc.getInteger("NLINK");
        }
        if (doc.containsKey("length")) {
            this.length = doc.getLong("length");
        }
        if (doc.containsKey("usedspace")) {
            this.usedspace = doc.getLong("usedspace");
        }
        if (doc.get("blocks") != null) {
            List ls = (List) doc.get("blocks");
            this.blockList = new byte[ls.size()][];
            int index = 0;
            for (Object obj : ls) {
                this.blockList[index++] = ((Binary) obj).getData();
            }
        }
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("_id", new Binary(VHW));
        doc.append("length", this.length);
        doc.append("usedspace", this.usedspace);
        doc.append("VNU", VNU);
        doc.append("NLINK", NLINK);
        List<Binary> blks = new ArrayList();
        if (blockList != null) {
            for (byte[] bs : blockList) {
                blks.add(new Binary(bs));
            }
        }
        doc.append("blocks", blks);
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
     * @return the VHW
     */
    public byte[] getVHW() {
        return VHW;
    }

    /**
     * @param VHW the VHW to set
     */
    public void setVHW(byte[] VHW) {
        this.VHW = VHW;
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
