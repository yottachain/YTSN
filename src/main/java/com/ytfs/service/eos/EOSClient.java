package com.ytfs.service.eos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ytfs.service.ServerConfig;
import com.ytfs.service.UserConfig;
import io.jafka.jeos.core.response.chain.transaction.PushedTransaction;
import java.util.Map;
import org.bson.types.ObjectId;

public class EOSClient {

    /**
     * 该用户是否有足够的HDD用于存储该数据最短存储时间PMS（例如60天）
     *
     * @param length 数据长度
     * @param reqdata
     * @param id
     * @return true：有足够空间，false：没有
     * @throws java.lang.Throwable
     */
    public static boolean hasSpace(long length, byte[] reqdata, ObjectId id) throws Throwable {
        PushedTransaction pts = EOSRequest.request(reqdata, id);
        String console = pts.getProcessed().getActionTraces().get(0).getConsole();
        ObjectMapper mapper = new ObjectMapper();
        Map readValue = mapper.readValue(console, Map.class);
        Object obj = readValue.get("balance");
        long balance = (obj instanceof Integer) ? (int) obj : (long) obj;
        long unitcount = length / UserConfig.Default_Shard_Size;
        long remain = length % UserConfig.Default_Shard_Size > 0 ? 1 : 0;
        long needcost = ServerConfig.unitcost * (unitcount + remain);
        return balance > needcost;
    }

    public static void deductHDD(byte[] reqdata, ObjectId id) throws Throwable {
        EOSRequest.request(reqdata, id);
    }

}
