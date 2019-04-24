package com.ytfs.service.codec;

import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import static io.jafka.jeos.util.KeyUtil.secp;
import io.jafka.jeos.util.Raw;
import io.jafka.jeos.util.ecc.Curve;
import io.jafka.jeos.util.ecc.Hex;
import io.jafka.jeos.util.ecc.Point;
import java.math.BigInteger;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECFieldF2m;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.EllipticCurve;
import javax.crypto.Cipher;
import javax.crypto.NullCipher;
import sun.security.ec.ECParameters;
import sun.security.ec.ECPrivateKeyImpl;
import sun.security.ec.ECPublicKeyImpl;

public class Test {

    public static void main(String[] args) throws Exception {
        byte[] data = "sdsdsdgfvs".getBytes();

        String s=KeyUtil.toPublicKey("5KQKydL7TuRwjzaFSK4ezH9RUXWuYHW1yYDp5CmQfsfTuu9MBLZ");
        byte[] k1 = Base58.decode(s.substring(3));
        byte[] bs1 = KeyStoreCoder.rsaEncryped(data, k1);

        byte[] k2 = Base58.decode("5KQKydL7TuRwjzaFSK4ezH9RUXWuYHW1yYDp5CmQfsfTuu9MBLZ");
        byte[] bs2 = KeyStoreCoder.rsaDecryped(bs1, k2);

        System.out.println(new String(bs2));
        if (true) {
            return;

        }

        BigInteger bi = privateKey("5KQKydL7TuRwjzaFSK4ezH9RUXWuYHW1yYDp5CmQfsfTuu9MBLZ");
        String pubkey = KeyUtil.toPublicKey("5KQKydL7TuRwjzaFSK4ezH9RUXWuYHW1yYDp5CmQfsfTuu9MBLZ");
        System.out.println(pubkey);

        ECPoint ecp = initECPoint(pubkey);

        // ecp=new ECPoint( new BigInteger(  
        //        "2fe13c0537bbc11acaa07d793de4e6d5e5c94eee8", 16),new BigInteger(  
        //       "289070fb05d38ff58321f2e800536d538ccdaa3d9", 16));
        ECParameterSpec espec = initECParameterSpec(ecp);
        ECPublicKey pubKey = initPublicKey(ecp, espec);
        ECPrivateKey priKey = initPrivateKey(bi, espec);
        // initKey();
        /*
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("EC");
        ECGenParameterSpec ecGenParameterSpec = new ECGenParameterSpec("secp256k1");
        keyPairGenerator.initialize(ecGenParameterSpec, new SecureRandom());
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        ECPrivateKeyImpl key1 = (ECPrivateKeyImpl) keyPair.getPrivate();
        BigInteger ss = key1.getS();
         */
        //ECPublicKeySpec ecPublicKeySpec = new ECPublicKeySpec(pubKey.getW(),
        //        pubKey.getParams());
        Cipher cipher = new NullCipher();
        cipher.doFinal(data);
        cipher.init(Cipher.ENCRYPT_MODE, pubKey, pubKey.getParams());
        byte[] bs = cipher.doFinal(data);
        //   ECPrivateKeySpec ecPrivateKeySpec = new ECPrivateKeySpec(priKey.getS(),
        //     priKey.getParams());
        Cipher cipher2 = new NullCipher();
        cipher2.init(Cipher.DECRYPT_MODE, priKey, priKey.getParams());
        //byte[] bs2 = cipher.doFinal(data);
       // System.out.println(new String(bs2));
        //KeyUtil.signHash(ss, data);
    }

    private static BigInteger privateKey(String pk) {
        byte[] private_wif = io.jafka.jeos.util.Base58.decode(pk);
        byte version = (byte) 0x80;
        if (private_wif[0] != version) {
            throw new IllegalArgumentException("Expected version " + 0x80 + ", instead got " + version);
        }
        byte[] private_key = Raw.copy(private_wif, 0, private_wif.length - 4);
        // byte[] new_checksum = SHA.sha256(private_key);
        //  new_checksum = SHA.sha256(new_checksum);
        //  new_checksum = Raw.copy(new_checksum, 0, 4);
        byte[] last_private_key = Raw.copy(private_key, 1, private_key.length - 1);
        BigInteger d = new BigInteger(Hex.toHex(last_private_key), 16);
        return d;
    }

    private static ECPoint initECPoint(String pk) {
        String ss = pk.substring(3);
        byte[] public_wif = io.jafka.jeos.util.Base58.decode(ss);
        byte[] public_key = Raw.copy(public_wif, 0, public_wif.length - 4);
        Curve curve = secp.getCurve();
        Point p = curve.decodePoint(public_key);
        ECPoint ep = new ECPoint(p.getX().toBigInteger(), p.getY().toBigInteger());
        return ep;
    }

    public static ECParameterSpec initECParameterSpec(ECPoint g) {
        // the order of generator
        BigInteger n = new BigInteger(
                "5846006549323611672814741753598448348329118574063", 10);
        n = new BigInteger(
                "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16);

        // the cofactor
        int h = 2;
        int m = 163;
        int[] ks = {7, 6, 3};
        ECFieldF2m ecField = new ECFieldF2m(m, ks);

        // y^2+xy=x^3+x^2+1
        BigInteger a = new BigInteger("0", 16); //new BigInteger(A, 16);// new BigInteger("1", 2);

        BigInteger b = new BigInteger("7", 16);  //new BigInteger(B, 16);//new BigInteger("1", 2);

        EllipticCurve ellipticCurve = new EllipticCurve(ecField, a, b);

        ECParameterSpec ecParameterSpec = new ECParameterSpec(ellipticCurve, g,
                n, h);
        ECParameters s;

        return ecParameterSpec;
    }

    public static ECPublicKey initPublicKey(ECPoint g, ECParameterSpec ecParameterSpec) throws Exception {
        // 公钥
        ECPublicKey publicKey = new ECPublicKeyImpl(g, ecParameterSpec);
        return publicKey;

    }

    public static ECPrivateKey initPrivateKey(BigInteger s, ECParameterSpec ecParameterSpec) throws Exception {
        // 私钥
        ECPrivateKey privateKey = new ECPrivateKeyImpl(s, ecParameterSpec);
        return privateKey;

    }

}
