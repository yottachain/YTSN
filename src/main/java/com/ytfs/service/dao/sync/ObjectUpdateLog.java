package com.ytfs.service.dao.sync;

import org.bson.types.ObjectId;

public class ObjectUpdateLog {
    
    private long usedspace;
    private byte[] block;
    private String VNU;

    /**
     * @return the VNU
     */
    public String getVNU() {
        return VNU;
    }

    /**
     * @param VNU the VNU to set
     */
    public void setVNU(ObjectId VNU) {
        this.VNU = VNU.toHexString();
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
     * @return the block
     */
    public byte[] getBlock() {
        return block;
    }

    /**
     * @param block the block to set
     */
    public void setBlock(byte[] block) {
        this.block = block;
    }

}
