package com.ytfs.service;

import io.jafka.jeos.util.KeyUtil;
import io.yottachain.p2phost.YottaP2P;

public class MakeNodeid {

    public static void main(String[] args) {
        try {
            String privatekey = KeyUtil.createPrivateKey();
            YottaP2P.start(9999, privatekey);
            System.out.println("      \"PrivateKey\":\"" + privatekey + "\",");
            System.out.println("      \"ID\": \"" + YottaP2P.id() + "\",");
            System.exit(0);
        } catch (Exception r) {
        }
    }
}
