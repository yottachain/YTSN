package com.ytfs.service.dao;

import static com.ytfs.service.ServerConfig.REDIS_EXPIRE;
import com.ytfs.service.packet.QueryObjectMetaReq;
import com.ytfs.service.packet.SerializationUtil;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.servlet.UploadBlockCache;
import com.ytfs.service.servlet.UploadObjectCache;
import com.ytfs.service.servlet.UploadShardCache;
import static com.ytfs.service.utils.Function.long2bytes;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import org.bson.types.ObjectId;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.BinaryJedisCluster;

public class RedisTest {

    public static void main(String[] a) throws Exception {
        int userid = 1;
        ObjectId VNU = new ObjectId("5cc3df0651f96b62b9b59d13");

        RedisSource source = null;
        try {
            source = RedisSource.getSource();
            BasicCommands jedis = source.getJedis();

            byte[] bs = (jedis instanceof BinaryJedis)
                    ? ((BinaryJedis) jedis).get(VNU.toByteArray())
                    : ((BinaryJedisCluster) jedis).get(VNU.toByteArray());
            UploadObjectCache cache;
            if (bs == null) {
                QueryObjectMetaReq req = new QueryObjectMetaReq();
                req.setUserID(userid);
                req.setVNU(VNU);

                cache = new UploadObjectCache();
                cache.setFilesize(1283247);
                cache.setUserid(userid);
                if (jedis instanceof BinaryJedis) {
                    ((BinaryJedis) jedis).setex(VNU.toByteArray(), REDIS_EXPIRE, SerializationUtil.serializeNoId(cache));
                } else {
                    ((BinaryJedisCluster) jedis).setex(VNU.toByteArray(), REDIS_EXPIRE, SerializationUtil.serializeNoId(cache));
                }
                short[] num = new short[]{0, 1};
                cache.setBlockNums(VNU, num);
            } else {
                cache = (UploadObjectCache) SerializationUtil.deserializeNoId(bs, UploadObjectCache.class);
                if (cache.getUserid() != userid) {
                    throw new ServiceException(INVALID_UPLOAD_ID);
                }
                // cache.setBlockNum(VNU, (short)4);

                cache.exists(VNU, (short) 0);
                cache.exists(VNU, (short) 5);
            }

            long VBI = 23423534634L;
            UploadBlockCache ca = new UploadBlockCache();
            ca.setNodes(new int[]{1, 2, 3, 4});
            ca.setShardcount(4);
            ca.setUserKey("sdsf".getBytes());
            ca.setVNU(VNU);

            if (jedis instanceof BinaryJedis) {
                // ((BinaryJedis) jedis).setex(long2bytes(VBI), REDIS_BLOCK_EXPIRE, SerializationUtil.serializeNoId(ca));
                //((BinaryJedis) jedis).setex(UploadBlockCache.getCacheKey1(VBI), REDIS_BLOCK_EXPIRE, new byte[0]);
            } else {
                // ((BinaryJedisCluster) jedis).setex(long2bytes(VBI), REDIS_BLOCK_EXPIRE, SerializationUtil.serializeNoId(ca));
                // ((BinaryJedisCluster) jedis).setex(UploadBlockCache.getCacheKey1(VBI), REDIS_BLOCK_EXPIRE, new byte[0]);
            }

            byte[] bss = (jedis instanceof BinaryJedis)
                    ? ((BinaryJedis) jedis).get(long2bytes(VBI))
                    : ((BinaryJedisCluster) jedis).get(long2bytes(VBI));

            ca = (UploadBlockCache) SerializationUtil.deserializeNoId(bss, UploadBlockCache.class);

            UploadShardCache shardCache = new UploadShardCache();
            shardCache.setRes(102);
            shardCache.setShardid(2);
            shardCache.setNodeid(0);
            shardCache.setVHF(hash("sdfsfdsf"));
            if (jedis instanceof BinaryJedis) {
                ((BinaryJedis) jedis).append(UploadBlockCache.getCacheKey1(VBI), shardCache.toByte());
            } else {
                ((BinaryJedisCluster) jedis).append(UploadBlockCache.getCacheKey1(VBI), shardCache.toByte());
            }

            byte[] data = (jedis instanceof BinaryJedis)
                    ? ((BinaryJedis) jedis).get(UploadBlockCache.getCacheKey1(VBI))
                    : ((BinaryJedisCluster) jedis).get(UploadBlockCache.getCacheKey1(VBI));
            if (data == null || data.length == 0) {

            }
            ByteBuffer buf = ByteBuffer.wrap(data);
            int len = data.length / 44;
            Map map = new HashMap();
            for (int ii = 0; ii < len; ii++) {
                UploadShardCache cache1 = new UploadShardCache();
                cache1.fill(buf);
                map.put(cache1.getShardid(), cache1);
            }

        } finally {
            if (source != null) {
                RedisSource.backSource(source);
            }
        }
    }

    private static byte[] hash(String ss) throws NoSuchAlgorithmException {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        return sha256.digest(ss.getBytes());
    }
}
