日志编码使用protobuf格式
syntax = "proto3";
message SyncDBLog {
   uint32 opName=1; 
   repeated bytes params=2;  
}

opName：表示操作类型，
举例，opName=1，表示插入数据块去重表
params：具体操作所涉及的各个参数

应用层在写入数据库时，先定义一个不重复的opName标识，按约定顺序填入各个参数
然后将SyncDBLog实例序列化，将序列化数据通过Http post发送到同步链

1.在对象表中（objects）中初始化一个文件的元数据

  opName=1

  objects的表结构
  字段名（_id）,byte[36],mongo中的Binary类型
  字段名（length）,long（即8字节64INT）
  字段名（usedspace）,long（即8字节64INT）
  字段名（VNU）,ObjectId类型
  字段名（NLINK）,int（即4字节64INT）
  字段名（blocks）,Binary类型数组Arrays
 

  初始化一个文件的元数据时，下面3个字段的初始值
  字段usedspace=0
  字段NLINK=0
  字段blocks=空的数组Arrays
  所以在同步日志消息中不包含上面三个参数

  params的定义
  params[0]:对应字段名（_id）,byte[36],使用Binary类型存入
  params[1]:对应字段名（length）,byte[8],转为int64类型存入
  params[2]:对应字段名（VNU）,byte[12],转为ObjectId类型存入

  写入数据库的逻辑
  生成一个包含六个字段的文档（document）写入mongodb   
  由于表objects对VNU做了唯一索引，捕捉“dup key”冲突，并忽略此错误。


2.
