package com.ytfs.service;

import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.File;

public class UserConfig {
    //*************************不可配置参数*************************************

    //对大文件分块时,内存中保留6M数据,避免写磁盘
    public final static int Max_Memory_Usage = 1024 * 1024 * 6;

    //对大文件分块时,数据块大小
    public final static int Default_Block_Size = 1024 * 1024 * 2 - 16 - 1;//PBL

    //数据块压缩时,空出8K以备END输出
    public final static int Compress_Reserve_Size = 16 * 1024;

    //数据分片大小
    public final static int Default_Shard_Size = 1024 * 16;  //PFL

    //最多容许掉线分片数量　
    public final static int Default_PND = 16;

    //小于PL2的数据块，直接记录在元数据库中
    public final static int PL2 = 256;

    //上传线程数
    public final static int UPLOADSHARDTHREAD = 50;

    //下载线程数
    public final static int DOWNLOADSHARDTHREAD = 50;

    //**************************可配置参数********************************
    //用户ID
    public static int userID;

    //用户对应的超级节点
    public static SuperNode superNode;

    //用户加密公钥
    public static byte[] KUEp;

    //用户签名私钥
    public static byte[] KUSp;

    //端口
    public static int port;
    
    //缓存路径
    public static File tmpFilePath;

}
