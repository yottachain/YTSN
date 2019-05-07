package com.ytfs.service.codec;

import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import static io.jafka.jeos.util.KeyUtil.secp;
import io.jafka.jeos.util.Raw;
import io.jafka.jeos.util.ecc.Curve;
import io.jafka.jeos.util.ecc.Hex;
import io.jafka.jeos.util.ecc.Point;
import java.math.BigInteger;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.PublicKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECFieldF2m;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.EllipticCurve;
import javax.crypto.Cipher;
import javax.crypto.NullCipher;
import sun.security.ec.ECPrivateKeyImpl;
import sun.security.ec.ECPublicKeyImpl;
import sun.security.util.ECUtil;

public class Test {
//根据EOS私钥生成Java的PrivateKey对象

    public static PrivateKey getPrivateKeyFromEOS(String pk) throws Exception {
        BigInteger iKey = privateKey(pk);
        ECPrivateKeyImpl priKey = new ECPrivateKeyImpl(iKey, ECUtil.getECParameterSpec((Provider) null, "secp256k1"));
        return priKey;
    }

//根据EOS公钥生成Java的PublicKey对象，参数中传入的EOS公钥需要去掉YTA/EOS的公钥前缀
    public static PublicKey getPublicKEyFromEOS(String pk) throws Exception {
        byte[] addr_buf = Base58.decode(pk);
        byte[] pub_buf = Raw.copy(addr_buf, 0, addr_buf.length - 4);
        Point ep = secp.getCurve().decodePoint(pub_buf);
        ECPoint ecpoint = new ECPoint(ep.getX().toBigInteger(), ep.getY().toBigInteger());
        ECPublicKeyImpl puk = new ECPublicKeyImpl(ecpoint, ECUtil.getECParameterSpec((Provider) null, "secp256k1"));
        return puk;
    }

    public static void testFunction() throws Exception {
        // 需要签名的数据
        byte[] data = new byte[1000];

        PrivateKey priKey = getPrivateKeyFromEOS("5JnSWYjcrAJSYe2huDRsofaPXwbing4LruUKDT1kyYLxuHmZCFk");
        PublicKey pubKey = getPublicKEyFromEOS("6cE9Xyx5JygnPFemAZkx761Pt6LZD5M6ZLD9B1PAHXFcnbruKQ");

        for (int i = 0; i < data.length; i++) {
            data[i] = 0xa;
        }

        //byte[] sign = signData("SHA256withECDSA", data, priKey);
        // boolean ret = verifySign("SHA256withECDSA", data, pubKey, sign);
        //data[1] = 0xb;
        //ret = verifySign("SHA256withECDSA", data, pubKey, sign);
    }

    public static void main(String[] args) throws Exception {
        byte[] kuep = Base58.decode("GZsJqUv51pw4c5HnBHiStK3jwJKXZjdtxVwkEShR9Ljb7ZUN1T");//公钥
        byte[] kusp = Base58.decode("5KQKydL7TuRwjzaFSK4ezH9RUXWuYHW1yYDp5CmQfsfTuu9MBLZ");//si钥       

        BigInteger bi = privateKey("5KQKydL7TuRwjzaFSK4ezH9RUXWuYHW1yYDp5CmQfsfTuu9MBLZ");
        String pubkey = KeyUtil.toPublicKey("5KQKydL7TuRwjzaFSK4ezH9RUXWuYHW1yYDp5CmQfsfTuu9MBLZ");
        System.out.println(pubkey);

        Point ecp = initECPoint(pubkey);

        // ecp=new ECPoint( new BigInteger(  
        //        "2fe13c0537bbc11acaa07d793de4e6d5e5c94eee8", 16),new BigInteger(  
        //       "289070fb05d38ff58321f2e800536d538ccdaa3d9", 16));
        ECParameterSpec espec = initECParameterSpec(ecp);
        //ECPublicKey pubKey = initPublicKey(ecp, espec);
        //pubKey.getEncoded();
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
        // cipher.doFinal(data);
        // cipher.init(Cipher.ENCRYPT_MODE, pubKey, pubKey.getParams());
        //byte[] bs = cipher.doFinal(data);
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

    private static Point initECPoint(String pk) {
        String ss = pk.substring(3);
        byte[] public_wif = io.jafka.jeos.util.Base58.decode(ss);
        byte[] public_key = Raw.copy(public_wif, 0, public_wif.length - 4);
        Curve curve = secp.getCurve();
        Point p = curve.decodePoint(public_key);
        return p;
    }

    public static ECParameterSpec initECParameterSpec(Point g) {
        // the order of generator
        BigInteger n = new BigInteger("5846006549323611672814741753598448348329118574063", 10);
        // the cofactor
        int h = 2;
        int m = 163;
        int[] ks = {7, 6, 3};
        ECFieldF2m ecField = new ECFieldF2m(m, ks);

        // y^2+xy=x^3+x^2+1
        BigInteger a = new BigInteger("1", 2); //new BigInteger(A, 16);// new BigInteger("1", 2);

        BigInteger b = new BigInteger("1", 2);  //new BigInteger(B, 16);//new BigInteger("1", 2);

        //EllipticCurve ellipticCurve = new EllipticCurve(ecField, a, b);
        EllipticCurve ellipticCurve = new EllipticCurve(ecField, a, b);

        ECPoint gg = new ECPoint(g.getX().toBigInteger(), g.getY().toBigInteger());

        ECParameterSpec ecParameterSpec = new ECParameterSpec(ellipticCurve, gg, n, h);
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
        // ECPublicKey publicKey = new ECPublicKeyImpl(ecParameterSpec.getGenerator(), ecParameterSpec);
        return privateKey;
    }

}
