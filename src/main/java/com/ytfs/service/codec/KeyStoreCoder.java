package com.ytfs.service.codec;

import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import java.security.Key;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.spec.PKCS8EncodedKeySpec;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;

public class KeyStoreCoder {

    /**
     * 生成随机密钥
     *
     * @return
     */
    public static byte[] generateRandomKey() {
        long r = Math.round(Math.random() * Long.MAX_VALUE);
        long l = System.currentTimeMillis();
        byte[] bs = new byte[]{(byte) (r >>> 56), (byte) (r >>> 48), (byte) (r >>> 40), (byte) (r >>> 32),
            (byte) (r >>> 24), (byte) (r >>> 16), (byte) (r >>> 8), (byte) (r),
            (byte) (l >>> 56), (byte) (l >>> 48), (byte) (l >>> 40), (byte) (l >>> 32),
            (byte) (l >>> 24), (byte) (l >>> 16), (byte) (l >>> 8), (byte) (l)
        };
        return bs;
    }

    /**
     * 加密
     *
     * @param data
     * @param kd 去重用密钥KD
     * @return ked 32字节
     */
    public static byte[] encryped(byte[] data, byte[] kd) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(kd, "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            return cipher.doFinal(data);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    /**
     * 解密
     *
     * @param data
     * @param kd 去重用密钥KD
     * @return ks 16字节
     */
    public static byte[] decryped(byte[] data, byte[] kd) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(kd, "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            return cipher.doFinal(data);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public static Key rsaPrivateKey1(byte[] prikey) {
        try {
            PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(prikey);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(pkcs8KeySpec);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public static Key rsaPublicKey1(byte[] pubkey) {
        try {
            PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(pubkey);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePublic(pkcs8KeySpec);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public static byte[] rsaEncryped(byte[] data, byte[] pubkey) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] key = sha256.digest(pubkey);
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            return cipher.doFinal(data);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }

        /*
        try {
            Key publicKey = rsaPublicKey(pubkey);
            Cipher cipher = Cipher.getInstance(publicKey.getAlgorithm());
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            return cipher.doFinal(data);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }*/
    }

    public static byte[] rsaDecryped(byte[] data, byte[] prikey) {
        try {
            String s = Base58.encode(prikey);
            String p = KeyUtil.toPublicKey(s);
            byte[] bs = Base58.decode(p.substring(3));
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] key = sha256.digest(bs);
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            return cipher.doFinal(data);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }

        /*
        try {
            Key privateK = rsaPrivateKey(prikey);
            Cipher cipher = Cipher.getInstance(privateK.getAlgorithm());
            cipher.init(Cipher.DECRYPT_MODE, privateK);
            return cipher.doFinal(data);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }*/
    }

}
