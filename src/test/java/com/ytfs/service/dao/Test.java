package com.ytfs.service.dao;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.bson.types.ObjectId;

public class Test {

    public static void main(String[] arg) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode node=mapper.createArrayNode();
        ObjectNode map=node.addObject();
  
        map.put("userId", 1);
        map.put("userName", "sanlsd");
        map.put("spaceTotal", 121L);
        ObjectNode map1 = node.addObject();
        map1.put("userId", 2);
        map1.put("userName", "sdfgg");
        map1.put("spaceTotal", 1241L); 
       
        String json = mapper.writeValueAsString(node);
        System.out.println("使用树型模型构建的json:"+json);
         
       // com.fasterxml.jackson.databind.
    }

    public static byte[] makeBytes(int length) {
        Random ran = new Random();
        ByteBuffer buf = ByteBuffer.allocate(length);
        for (int ii = 0; ii < length / 8; ii++) {
            long l = ran.nextLong();
            buf.putLong(l);
        }
        return buf.array();
    }

    private static void testFile() throws Exception {
        ObjectId bucketId = new ObjectId("5ce6613551f96b0c6a8f1b58");
        String filename = "dir/dfile4";
        FileMetaV2 meta = new FileMetaV2(new ObjectId(), bucketId, filename);

        // FileAccessorV2.insertFileMeta(meta);
        ObjectId verid = new ObjectId("5cf7463651010218385ae602");
        // FileAccessorV2.deleteFileMeta(bucketId, filename, verid);
        //FileMetaV2 newmeta = FileAccessorV2.getFileMeta(bucketId, filename, verid);

        //FileMetaV2 newmeta = FileAccessorV2.getFileMeta(bucketId, filename);
        //System.out.println(newmeta.getVersionId());
    }

    private static void testObjectLs() throws Exception {
        int limit = 200;
        ObjectId bucketId = new ObjectId("5ce6613551f96b0c6a8f1b58");
        // long count = FileAccessorV2.getObjectCount(bucketId);
        // System.out.println("count:" + count);

        //List<FileMetaV2> ls = FileAccessorV2.listBucket(bucketId, null, FileAccessorV2.firstVersionId, "dir\\", limit);
        // System.out.println(ls.size());
    }

    private static void testUser() throws Exception {
        User usr = new User(Sequence.generateUserID());
        String prikey = "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx";
        //byte[] kusp = Base58.decode(prikey);//si钥    
        String ss = KeyUtil.toPublicKey(prikey);
        String pubkey = ss.substring(3);
        System.out.println(pubkey);
        byte[] kuep = Base58.decode(pubkey);
        usr.setKUEp(kuep);
        UserAccessor.addUser(usr);
    }

}
