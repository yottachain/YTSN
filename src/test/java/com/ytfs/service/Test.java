package com.ytfs.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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

public class Test {

    public static void main(String[] args) throws Exception {


        // --- get the current state of blockchain
        EosApi eosApi = EosApiFactory.create("http://152.136.11.202:8888/");
        SignArg arg = eosApi.getSignArg(120);
        System.out.println(eosApi.getObjectMapper().writeValueAsString(arg));

        // --- sign the transation of token tansfer
        //String privateKey = "5KQwrPbwdL6PhXujxW37FSSQZ1JiwsST4cqQzDeyXtP79zkvFD3";//replace the real private key
        String privateKey = "5JUB5GKXU68YyXERgEPoRJ9xWCNqL8xhUzhyK12GsdZpzQdozGa";//replace the real private key
        String from = "hddpool12345";
        LocalApi localApi = EosApiFactory.createLocalApi();
        //PushTransactionRequest req = localApi.transfer(arg, privateKey, from1, to1, quantity, memo);

        // ① pack transfer data
        String name = "username1234";
        Raw raw = new Raw();
        
        raw.packName(name);
        raw.packUint64(5);
        String transferData = raw.toHex();
        //

        // ③ create the authorization
        List<TransactionAuthorization> authorizations = Arrays.asList(new TransactionAuthorization(from, "active"));

        // ④ build the all actions
        List<TransactionAction> actions = Arrays.asList(//getbalance
                new TransactionAction("hddpool12345", "getbalance", authorizations, transferData)//
        );

        // ⑤ build the packed transaction
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

        System.out.println(localApi.getObjectMapper().writeValueAsString(req));

        // --- push the signed-transaction to the blockchain
        PushedTransaction pts = eosApi.pushTransaction(req);
        
         
        System.out.println(localApi.getObjectMapper().writeValueAsString(pts));
        /*
        ObjectMapper mapper = new ObjectMapper();
        Map readValue = mapper.readValue(console, Map.class); 
       // int balance = Integer.parseInt(readValue.get("balance"));
        System.out.println(readValue.get("balance"));
        */
       // System.out.println(localApi.getObjectMapper().writeValueAsString(pts));

    }

    private static String sign(String privateKey, SignArg arg, PackedTransaction t) {
        Raw raw = Packer.packPackedTransaction(arg.getChainId(), t);
        raw.pack(ByteBuffer.allocate(33).array());
        String hash = KeyUtil.signHash(privateKey, raw.bytes());
        return hash;
    }
}
