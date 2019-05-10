package com.ytfs.service;

public class ServerConfig {

    //*************************不可配置参数*************************************
    //每个文件在上传后必须存储的最短周期，如1个周期
    public final static long PMS = 1;

    //单位空间（如16K）周期费用，如100000000 nHDD=1G使用365天，如果每周期60天
    public static long unitcost = 251;

    //计费周期如:60天
    public static long PPC = 1000 * 60 * 60 * 24 * 60;

    //元数据空间
    public final static long PCM = 16 * 1024;

    //REDIS存储key的过期时间
    public final static int REDIS_EXPIRE = 60 * 60 * 10;

    //REDIS存储key的过期时间
    public final static int REDIS_BLOCK_EXPIRE = 60 * 60;

    //小于PL2的数据块，直接记录在元数据库中
    public final static int PL2 = 256;

    //存储节点验签失败,拒绝存储,超过3次,惩罚
    public final static int PNF = 3;
 
    //**************************可配置参数********************************
    //服务端超级节点编号,本服务节点编号
    public static int superNodeID;

    //超级节点私钥
    public static String privateKey;
    public static byte[] SNDSP;

    //端口
    public static int port = 9999;

    //eos ADD
    public static String eosURI;

    //端口
    public static int httpPort = 8080;

    //http绑定ip
    public static String httpBindip = "";

}
