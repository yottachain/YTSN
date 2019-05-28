package com.ytfs.service.dao;

import com.ytfs.common.LogConfigurator;
import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.bson.types.ObjectId;

public class Test {

    public static void main(String[] a) throws Exception {
        LogConfigurator.configPath();
        //UserAccessor.total();
        //testRedis();
        //testSeq();     
      //  testObjectLs();
        //testObject();
    }

    /*
    private static void testObjectLs() throws Exception {
        int limit = 200;
        ObjectId bucketId = new ObjectId("5ceb533995adab7f02dc4d41");
        long count = FileAccessor.totalObjectsByBucket(bucketId);
        System.out.println("count:" + count);
        ObjectId startId = null;
        Map<String, byte[]> map = new HashMap();
        long total = 0;
        while (true) {
            map.clear();
            startId = FileAccessor.listObjectByBucket(map, bucketId, startId, limit);
            Set<Map.Entry<String, byte[]>> set = map.entrySet();
            for (Map.Entry<String, byte[]> ent : set) {
                System.out.println(ent.getKey());
            }
            total = total + map.size();
            System.out.println("***************" + map.size() + "*********************");
            if (startId == null) {
                break;
            }
        }
        System.out.println("******total *********" + total + "*********************");
    }
*/
    private static void testSeq() throws Exception {
        //int uid = Sequence.getSequence(1);
        //System.out.println(Integer.toHexString(uid));

        long l = Sequence.generateBlockID(10);
        System.out.println(Long.toHexString(l));
        System.out.println(Long.toHexString(l + 1));
        System.out.println(Long.toHexString(l + 2));
        System.out.println(Long.toHexString(l + 3));
        System.out.println(Long.toHexString(l + 4));
        System.out.println(Long.toHexString(l + 5));
        System.out.println(Long.toHexString(l + 6));
        System.out.println(Long.toHexString(l + 7));
        System.out.println(Long.toHexString(l + 8));
        System.out.println(Long.toHexString(l + 9));
        System.out.println("********************************");
        l = Sequence.generateBlockID(10);
        System.out.println(Long.toHexString(l));
        System.out.println(Long.toHexString(l + 1));
        System.out.println(Long.toHexString(l + 2));
        System.out.println(Long.toHexString(l + 3));
        System.out.println(Long.toHexString(l + 4));
        System.out.println(Long.toHexString(l + 5));
        System.out.println(Long.toHexString(l + 6));
        System.out.println(Long.toHexString(l + 7));
        System.out.println(Long.toHexString(l + 8));
        System.out.println(Long.toHexString(l + 9));
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

    private static void testObject() throws Exception {
        ObjectMeta meta = new ObjectMeta(1, "sssssssssssgewggwgwg".getBytes());
        meta.setBlocks("fsdgdth".getBytes());
        meta.setNLINK(1);
        meta.setVNU(new ObjectId());
        //ObjectDAO.addObject(meta);

        ObjectAccessor.isObjectExists(meta);
        System.out.println("nlink:" + meta.getNLINK());
        System.out.println("VNU:" + meta.getVNU());

        //ObjectDAO.incObjectNLINK(meta);
        meta = ObjectAccessor.getObject(meta.getUserID(), meta.getVHW());
        System.out.println(meta.getUserID());
        System.out.println(new String(meta.getVHW()));
        System.out.println(new String(meta.getBlocks()));
        System.out.println(meta.getNLINK());
    }
}
