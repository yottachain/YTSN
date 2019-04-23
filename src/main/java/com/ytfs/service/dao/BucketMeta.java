package com.ytfs.service.dao;

import org.bson.Document;
import org.bson.types.ObjectId;

public class BucketMeta {

    private String bucketName;
    private int userId;
    private ObjectId bucketId;

    public BucketMeta(int userId, ObjectId bucketId, String bucketName) {
        this.userId = userId;
        this.bucketId = bucketId;
        this.bucketName = bucketName;
    }

    public BucketMeta(Document doc) {
        if (doc.containsKey("_id")) {
            this.bucketId = doc.getObjectId("_id");
        }
        if (doc.containsKey("userId")) {
            this.userId = doc.getInteger("userId");
        }
        if (doc.containsKey("bucketName")) {
            this.bucketName = doc.getString("bucketName");
        }
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("_id", bucketId);
        doc.append("userId", userId);
        doc.append("bucketName", bucketName);
        return doc;
    }

    /**
     * @return the bucketName
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * @param bucketName the bucketName to set
     */
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    /**
     * @return the userId
     */
    public int getUserId() {
        return userId;
    }

    /**
     * @param userId the userId to set
     */
    public void setUserId(int userId) {
        this.userId = userId;
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

}
