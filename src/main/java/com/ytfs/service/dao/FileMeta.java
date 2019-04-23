package com.ytfs.service.dao;

import org.bson.Document;
import org.bson.types.ObjectId;

public class FileMeta {

    private ObjectId fileId;
    private ObjectId bucketId;
    private String fileName;
    private ObjectId VNU;
    
    
    public FileMeta(){
        this.fileId=new ObjectId();
    }

    public FileMeta(Document doc) {
        if (doc.containsKey("_id")) {
            this.fileId = doc.getObjectId("_id");
        }
        if (doc.containsKey("bucketId")) {
            this.bucketId = doc.getObjectId("bucketId");
        }
        if (doc.containsKey("VNU")) {
            this.VNU = doc.getObjectId("VNU");
        }
        if (doc.containsKey("fileName")) {
            this.fileName = doc.getString("fileName");
        }
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("_id", fileId);
        doc.append("bucketId", bucketId);
        doc.append("VNU", VNU);
        doc.append("fileName", fileName);
        return doc;
    }

    /**
     * @return the fileId
     */
    public ObjectId getFileId() {
        return fileId;
    }

    /**
     * @param fileId the fileId to set
     */
    public void setFileId(ObjectId fileId) {
        this.fileId = fileId;
    }

    /**
     * @return the bucketId
     */
    public ObjectId getBucketId() {
        return bucketId;
    }

    /**
     * @param bucketId the bucketId to set
     */
    public void setBucketId(ObjectId bucketId) {
        this.bucketId = bucketId;
    }

    /**
     * @return the fileName
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * @param fileName the fileName to set
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
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
