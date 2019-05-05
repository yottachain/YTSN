package com.ytfs.service.eos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ytfs.service.ServerConfig;
import io.jafka.jeos.EosApi;
import io.jafka.jeos.EosApiFactory;
import io.jafka.jeos.LocalApi;
import io.jafka.jeos.convert.Packer;
import io.jafka.jeos.core.common.SignArg;
import io.jafka.jeos.core.common.transaction.PackedTransaction;
import io.jafka.jeos.core.common.transaction.TransactionAction;
import io.jafka.jeos.core.common.transaction.TransactionAuthorization;
import io.jafka.jeos.core.request.chain.transaction.PushTransactionRequest;
import io.jafka.jeos.core.response.chain.transaction.PushedTransaction;
import io.jafka.jeos.util.KeyUtil;
import io.jafka.jeos.util.Raw;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class EOSClient {

    private final String eosID;//eos帐户ID

    public EOSClient(String eosID) {
        this.eosID = eosID;
    }

    private String sign(String privateKey, SignArg arg, PackedTransaction t) {
        Raw raw = Packer.packPackedTransaction(arg.getChainId(), t);
        raw.pack(ByteBuffer.allocate(33).array());
        String hash = KeyUtil.signHash(privateKey, raw.bytes());
        return hash;
    }

    /**
     * 该用户是否有足够的HDD用于存储该数据最短存储时间PMS（例如60天）
     *
     * @param length 数据长度
     * @param PMS 最短存储时间PMS(单位ms)
     * @return true：有足够空间，false：没有
     * @throws java.lang.Throwable
     */
    public boolean hasSpace(long length, long PMS) throws Throwable {
        EosApi eosApi = EosApiFactory.create(ServerConfig.eosURI);
        SignArg arg = eosApi.getSignArg(120);
        String privateKey = ServerConfig.eosPrivateKey;
        String from = ServerConfig.eosBPAccount;
        LocalApi localApi = EosApiFactory.createLocalApi();
        String name = eosID;
        Raw raw = new Raw();
        raw.packName(name);
        String transferData = raw.toHex();
        List<TransactionAuthorization> authorizations = Arrays.asList(new TransactionAuthorization(from, "active"));
        List<TransactionAction> actions = Arrays.asList(//
                new TransactionAction(ServerConfig.contractAccount, "getbalance", authorizations, transferData)//
        );
        PackedTransaction packedTransaction = new PackedTransaction();
        packedTransaction.setExpiration(arg.getHeadBlockTime().plusSeconds(arg.getExpiredSecond()));
        packedTransaction.setRefBlockNum(arg.getLastIrreversibleBlockNum());
        packedTransaction.setRefBlockPrefix(arg.getRefBlockPrefix());

        packedTransaction.setMaxNetUsageWords(0);
        packedTransaction.setMaxCpuUsageMs(0);
        packedTransaction.setDelaySec(0);
        packedTransaction.setActions(actions);

        String hash = sign(privateKey, arg, packedTransaction);
        PushTransactionRequest req = new PushTransactionRequest();
        req.setTransaction(packedTransaction);
        req.setSignatures(Arrays.asList(hash));

        PushedTransaction pts = eosApi.pushTransaction(req);
        String console = pts.getProcessed().getActionTraces().get(0).getConsole();
        ObjectMapper mapper = new ObjectMapper();
        Map readValue = mapper.readValue(console, Map.class);
        int balance = 0;
        try {
            balance = (int) readValue.get("balance");
        } catch (NumberFormatException r) {
        }
        return true;
    }

    /**
     * 扣除相应的HDD
     *
     * @param length
     * @throws Throwable
     */
    public void deductHDD(long length) throws Throwable {

    }

    /**
     * 冻结该用户相应的HDD
     *
     * @param length
     * @throws Throwable
     */
    public void frozenHDD(long length) throws Throwable {
    }

    /**
     * 释放相应的HDD
     *
     * @param length
     * @throws Throwable
     */
    public void freeHDD(long length) throws Throwable {
    }

    /**
     * 没收该用户相应的HDD
     *
     * @param length
     * @throws Throwable
     */
    public void punishHDD(long length) throws Throwable {
    }

}
