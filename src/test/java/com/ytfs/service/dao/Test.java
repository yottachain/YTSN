package com.ytfs.service.dao;

import com.ytfs.common.LogConfigurator;
import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.types.ObjectId;

public class Test {

    public static void main(String[] a) throws Exception {

        //LogConfigurator.configPath();
        //UserAccessor.total();
        //testRedis();
        //testSeq();     
        testObjectLs();
        //testFile();
    }

    private static void testFile() throws Exception {
        ObjectId bucketId = new ObjectId("5ce6613551f96b0c6a8f1b58");
        String filename = "dir/file3";
        FileMetaV2 meta = new FileMetaV2(new ObjectId(), bucketId, filename);

        FileAccessorV2.saveFileMeta(meta);
        
        
        ObjectId verid = new ObjectId("5cf7463651010218385ae602");
       // FileAccessorV2.deleteFileMeta(bucketId, filename, verid);
        //FileMetaV2 newmeta = FileAccessorV2.getFileMeta(bucketId, filename, verid);

        //FileMetaV2 newmeta = FileAccessorV2.getFileMeta(bucketId, filename);
        //System.out.println(newmeta.getVersionId());
    }

    private static void testObjectLs() throws Exception {
        int limit = 200;
        ObjectId bucketId = new ObjectId("5ce6613551f96b0c6a8f1b58");
        long count = FileAccessorV2.getObjectCount(bucketId);
        System.out.println("count:" + count);
 
        List<FileMetaV2> ls = FileAccessorV2.listBucket(bucketId, null, null, null, limit);
        System.out.println(ls.size());
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
