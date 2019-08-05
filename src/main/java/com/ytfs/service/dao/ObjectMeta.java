package com.ytfs.service.dao;

import java.nio.ByteBuffer;
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
    private byte[] blocks;
    private byte[][] blockList;

    private final byte[] _id;

    public ObjectMeta(int userID, byte[] VHW) {
        this.userID = userID;
        this.VHW = VHW;
        byte[] id = new byte[VHW.length + 4];
        id[0] = (byte) (userID >>> 24);
        id[1] = (byte) (userID >>> 16);
        id[2] = (byte) (userID >>> 8);
        id[3] = (byte) (userID);
        System.arraycopy(VHW, 0, id, 4, VHW.length);
        this._id = id;
    }

    public ObjectMeta(Document doc) {
        if (doc.containsKey("_id")) {
            this._id = ((Binary) doc.get("_id")).getData();
            ByteBuffer bb = ByteBuffer.wrap(_id);
            this.userID = bb.getInt();
            VHW = new byte[bb.remaining()];
            bb.get(VHW);
        } else {
            _id = null;
        }
        fill(doc);
    }

    public final void fill(Document doc) {
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
            if (doc.get("blocks") instanceof List) {
                List ls = (List) doc.get("blocks");
                this.blockList = new byte[ls.size()][];
                int index = 0;
                for (Object obj : ls) {
                    this.blockList[index++] = ((Binary) obj).getData();
                }
            } else {
                this.blocks = ((Binary) doc.get("blocks")).getData();
            }
        }
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("_id", new Binary(getId()));
        doc.append("length", this.length);
        doc.append("usedspace", this.usedspace);
        doc.append("VNU", VNU);
        doc.append("NLINK", NLINK);
        if (blocks != null) {
            doc.append("blocks", new Binary(blocks));
        } else {
            List<Binary> blks = new ArrayList();
            if (blockList != null) {
                for (byte[] bs : blockList) {
                    blks.add(new Binary(bs));
                }
            }
            doc.append("blocks", blks);
        }
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
     * @return the blocks
     */
    public byte[] getBlocks() {
        return blocks;
    }

    /**
     * @param blocks the blocks to set
     */
    public void setBlocks(byte[] blocks) {
        this.blocks = blocks;
    }

    /**
     * @return the _id
     */
    public byte[] getId() {
        return _id;
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
