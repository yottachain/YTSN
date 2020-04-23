package com.ytfs.service.dao;

import io.yottachain.p2phost.utils.Base58;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.bson.types.ObjectId;

public class FileListCacheKey {

    final ObjectId bucketId;
    final String prefix;
    final boolean withVersion;
    final String pubkey;

    FileListCacheKey(String pubkey, ObjectId bucketId, String prefix, ObjectId nextVersionId) {
        this.pubkey = pubkey.toLowerCase().startsWith("eos") ? pubkey.substring(3) : pubkey;
        this.bucketId = bucketId;
        this.prefix = prefix == null ? "" : prefix.trim();
        this.withVersion = nextVersionId != null;
    }

    public String getKey() {
        byte[] bs4 = null;
        if (!prefix.equals("")) {
            bs4 = prefix.getBytes(Charset.forName("utf-8"));
        }
        byte[] bs1 = Base58.decode(pubkey);
        byte[] bs2 = bucketId.toByteArray();
        byte bs3 = withVersion ? (byte) 1 : (byte) 0;
        int len=bs1.length+bs2.length+1;
        len=bs4==null?len:(len+bs4.length);
        ByteBuffer bs = ByteBuffer.allocate(len );
        bs.put(bs1);
        bs.put(bs2);
        bs.put(bs3);
        if (bs4!=null) {
            bs.put(bs4);
        }
        bs.flip();
        return Base58.encode(bs.array());
    }
}
