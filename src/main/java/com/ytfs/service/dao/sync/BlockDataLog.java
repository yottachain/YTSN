package com.ytfs.service.dao.sync;

public class BlockDataLog {

    private long id;
    private byte[] dat;

    /**
     * @return the _id
     */
    public long getId() {
        return id;
    }

    /**
     * @param _id the _id to set
     */
    public void setId(long _id) {
        this.id = _id;
    }

    /**
     * @return the dat
     */
    public byte[] getDat() {
        return dat;
    }

    /**
     * @param dat the dat to set
     */
    public void setDat(byte[] dat) {
        this.dat = dat;
    }

}
