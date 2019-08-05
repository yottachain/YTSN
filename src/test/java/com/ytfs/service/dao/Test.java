package com.ytfs.service.dao;

import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import java.util.List;
import org.bson.types.ObjectId;

public class Test {

    public static void main(String[] a) throws Exception {
        int shardcount = 30;
        long l = Sequence.generateBlockID(shardcount);
        for (long ii = 0; ii < shardcount; ii++) {
            System.out.println(Long.toHexString(l++));
        }
        
        Sequence.initUserID_seq();
        
        int newid=Sequence.generateUserID();
         System.out.println(newid);
        // LogConfigurator.configPath(null,"EDBUG");
        //ServerConfig.superNodeID=3;
        // MongoSource.getMongoSource().init_seq_collection(21);
        //UserAccessor.total();
        //testRedis();
        //testSeq();     
        //testObjectLs();
        //testFile();
    }

    private static void testFile() throws Exception {
        ObjectId bucketId = new ObjectId("5ce6613551f96b0c6a8f1b58");
        String filename = "dir/dfile4";
        FileMetaV2 meta = new FileMetaV2(new ObjectId(), bucketId, filename);

        FileAccessorV2.insertFileMeta(meta);

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

        List<FileMetaV2> ls = FileAccessorV2.listBucket(bucketId, null, FileAccessorV2.firstVersionId, "dir\\", limit);
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
