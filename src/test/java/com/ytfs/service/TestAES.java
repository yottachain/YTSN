package com.ytfs.service;

import com.ytfs.service.codec.AESIVParameter;
import java.security.Key;
import java.security.MessageDigest;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;

public class TestAES {
//-----類別常數-----

    /**
     * 預設的Initialization Vector，為16 Bits的0
     */
  //  private static final IvParameterSpec DEFAULT_IV = new IvParameterSpec(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    /**
     * 加密演算法使用AES
     */
    private static final String ALGORITHM = "AES";
    /**
     * AES使用CBC模式與PKCS5Padding
     */
    private static final String TRANSFORMATION = "AES/CBC/PKCS5Padding";

//-----物件變數-----
    /**
     * 取得AES加解密的密鑰
     */
    private Key key;
    /**
     * AES CBC模式使用的Initialization Vector
     */
    private IvParameterSpec iv;
    /**
     * Cipher 物件
     */
    private Cipher cipher;

//-----建構子-----
    /**
     * 建構子，使用128 Bits的AES密鑰(計算任意長度密鑰的MD5)和預設IV
     *
     * @param key 傳入任意長度的AES密鑰
     */
    public TestAES(final String key) {
        this(key, 128);
    }

    /**
     * 建構子，使用128 Bits或是256 Bits的AES密鑰(計算任意長度密鑰的MD5或是SHA256)和預設IV
     *
     * @param key 傳入任意長度的AES密鑰
     * @param bit 傳入AES密鑰長度，數值可以是128、256 (Bits)
     */
    public TestAES(final String key, final int bit) {
        this(key, bit, null);
    }

    /**
     * 建構子，使用128 Bits或是256 Bits的AES密鑰(計算任意長度密鑰的MD5或是SHA256)，用MD5計算IV值
     *
     * @param key 傳入任意長度的AES密鑰
     * @param bit 傳入AES密鑰長度，數值可以是128、256 (Bits)
     * @param iv 傳入任意長度的IV字串
     */
    public TestAES(final String key, final int bit, final String iv) {
        if (bit == 256) {
            this.key = new SecretKeySpec(getHash("SHA-256", key), ALGORITHM);
        } else {
            this.key = new SecretKeySpec(getHash("MD5", key), ALGORITHM);
        }
        if (iv != null) {
           // this.iv = new IvParameterSpec(getHash("MD5", iv));
        } else {
           // this.iv = DEFAULT_IV;
        }
        this.iv=new IvParameterSpec(AESIVParameter.IVParameter);
        init();
    }
    
     public static void main(String[] args) throws Exception {
         TestAES aes=new TestAES("123456",256,"abcdef");
         
         String ss="1234567890abcdef";
         
         
         byte[] bs= aes.encrypt(ss.getBytes());
         
         System.out.println(Hex.encodeHexString(bs));
         
         byte[] bss=aes.decrypt(bs);
         System.out.println(new String(bss));
     }

//-----物件方法-----
    /**
     * 取得字串的雜湊值
     *
     * @param algorithm 傳入雜驟演算法
     * @param text 傳入要雜湊的字串
     * @return 傳回雜湊後資料內容
     */
    private static byte[] getHash(final String algorithm, final String text) {
        try {
            return getHash(algorithm, text.getBytes("UTF-8"));
        } catch (final Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    /**
     * 取得資料的雜湊值
     *
     * @param algorithm 傳入雜驟演算法
     * @param data 傳入要雜湊的資料
     * @return 傳回雜湊後資料內容
     */
    private static byte[] getHash(final String algorithm, final byte[] data) {
        try {
            final MessageDigest digest = MessageDigest.getInstance(algorithm);
            digest.update(data);
            return digest.digest();
        } catch (final Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    /**
     * 初始化
     */
    private void init() {
        try {
            cipher = Cipher.getInstance(TRANSFORMATION);
        } catch (final Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

 
    /**
     * 加密資料
     *
     * @param data 傳入要加密的資料
     * @return 傳回加密後的資料
     */
    public byte[] encrypt(final byte[] data) {
        try {
            cipher.init(Cipher.ENCRYPT_MODE, key, iv);
            final byte[] encryptData = cipher.doFinal(data);
            return encryptData;
        } catch (final Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }
 
    /**
     * 解密文字
     *
     * @param data 傳入要解密的資料
     * @return 傳回解密後的文字
     */
    public byte[] decrypt(final byte[] data) {
        try {
            cipher.init(Cipher.DECRYPT_MODE, key, iv);
            final byte[] decryptData = cipher.doFinal(data);
            return decryptData;
        } catch (final Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }
}
