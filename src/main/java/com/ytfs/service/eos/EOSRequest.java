package com.ytfs.service.eos;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ytfs.service.ServerConfig;
import io.jafka.jeos.EosApi;
import io.jafka.jeos.EosApiFactory;
import io.jafka.jeos.convert.Packer;
import io.jafka.jeos.core.common.SignArg;
import io.jafka.jeos.core.common.transaction.PackedTransaction;
import io.jafka.jeos.core.common.transaction.TransactionAction;
import io.jafka.jeos.core.common.transaction.TransactionAuthorization;
import io.jafka.jeos.core.request.chain.transaction.PushTransactionRequest;
import io.jafka.jeos.core.response.chain.transaction.PushedTransaction;
import io.jafka.jeos.util.KeyUtil;
import io.jafka.jeos.util.Raw;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.bson.types.ObjectId;

public class EOSRequest {

    private static byte[] encodeSignArg(SignArg req) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsBytes(req);
    }

    private static SignArg decodeSignArg(byte[] bs) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(bs, SignArg.class);
    }

    private static byte[] encodeRequest(PushTransactionRequest req) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsBytes(req);
    }

    private static PushTransactionRequest decodeRequest(byte[] bs) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(bs, PushTransactionRequest.class);
    }

    public static byte[] createEosClient(ObjectId id) throws JsonProcessingException {
        EosApi eosApi = EosApiFactory.create(ServerConfig.eosURI);
        SignArg arg = eosApi.getSignArg((int) EOSClientCache.EXPIRED_TIME);
        EOSClientCache.putClient(id, eosApi);
        return encodeSignArg(arg);
    }

    public static PushedTransaction request(byte[] reqdata, ObjectId id) throws IOException {
        EosApi eosApi = EOSClientCache.getClient(id);
        PushTransactionRequest req = decodeRequest(reqdata);
        return eosApi.pushTransaction(req);
    }

    public static byte[] makeSubBalanceRequest(byte[] signarg, String from, String privateKey, String contractAccount, long cost) throws JsonProcessingException, IOException {
        SignArg arg = decodeSignArg(signarg);
        Raw raw = new Raw();
        raw.packName(from);
        raw.packUint64(cost);
        String transferData = raw.toHex();
        List<TransactionAuthorization> authorizations = Arrays.asList(new TransactionAuthorization(from, "active"));
        List<TransactionAction> actions = Arrays.asList(
                new TransactionAction(contractAccount, "subbalance", authorizations, transferData)
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
        return encodeRequest(req);
    }

    public static byte[] makeGetBalanceRequest(byte[] signarg, String from, String privateKey, String contractAccount) throws JsonProcessingException, IOException {
        SignArg arg = decodeSignArg(signarg);
        Raw raw = new Raw();
        raw.packName(from);
        String transferData = raw.toHex();
        List<TransactionAuthorization> authorizations = Arrays.asList(new TransactionAuthorization(from, "active"));
        List<TransactionAction> actions = Arrays.asList(
                new TransactionAction(contractAccount, "getbalance", authorizations, transferData)
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
        return encodeRequest(req);
    }

    private static String sign(String privateKey, SignArg arg, PackedTransaction t) {
        Raw raw = Packer.packPackedTransaction(arg.getChainId(), t);
        raw.pack(ByteBuffer.allocate(33).array());
        String hash = KeyUtil.signHash(privateKey, raw.bytes());
        return hash;
    }
}
