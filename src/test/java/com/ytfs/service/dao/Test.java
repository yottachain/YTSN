package com.ytfs.service.dao;

import static io.grpc.netty.shaded.io.netty.channel.group.ChannelMatchers.is;
import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Random;
import org.bson.types.ObjectId;

public class Test {

    public static void main(String[] arg) throws Exception {
        InputStream is = null;
        BufferedReader br = null;
        String result = null;// 返回结果字符串
        URL url = new URL("http://localhost:8080/usertotal");
        // 通过远程url连接对象打开一个连接，强转成httpURLConnection类
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        // 设置连接方式：get
        connection.setRequestMethod("GET");
        // 设置连接主机服务器的超时时间：15000毫秒
        connection.setConnectTimeout(15000);
        // 设置读取远程返回的数据时间：60000毫秒
        connection.setReadTimeout(60000);
        // 发送请求
        connection.connect();
        // 通过connection连接，获取输入流
       // System.out.println(connection.getResponseMessage());
        
        if (connection.getResponseCode() == 200) {
            is = connection.getInputStream();
            // 封装输入流is，并指定字符集
            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            // 存放数据
            StringBuffer sbf = new StringBuffer();
            String temp = null;
            while ((temp = br.readLine()) != null) {
                sbf.append(temp);
                sbf.append("\r\n");
            }
            result = sbf.toString();
        } else {            
            is = connection.getErrorStream();
            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            // 存放数据
            StringBuffer sbf = new StringBuffer();
            String temp = null;
            while ((temp = br.readLine()) != null) {
                sbf.append(temp);
                sbf.append("\r\n");
            }
            result = sbf.toString();
        }
        System.out.println(result);
        // System.out.println(s);
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

        UserAccessor.addUser(usr);
    }

}
