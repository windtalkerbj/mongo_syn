# mongo_syn(v.01)
via canal(By Alibaba),decode MYSQL binlog content,then transform to MONGODB

1,设计目标：

  --将MYSQL实时数据变动、DDL变更同步到对应的MONGODB上
  
  --在MONGO上对数据做加工,形成类似统一视图（FUTURE TARGET）
  
  --适用于读写分离中的读场景


2，数据流向：
  MYSQL BINLOG->CANAL->KAFKA->MONGODB


3,启动说明
  python mongo_syn.py {TOPIC_NAME} {CONSUMER-GROUP-NAME} 


4,开发涉及资源
  Canal1.1.3
  Python3.7
  Kafka2.12
  pymongo/kafka-python


5,配置说明（db.cfg）

[MONGO_SYNC]

  log_path={日志输出目录}
  
  mongo_url={MONGOS地址}
  
  bootstrap_servers={KAFKA地址}
  
  from_offset={空 或 TOPIC.PARTITION_NUM:OFFSET}
  
    说明：如留空，则从上次COMMIT位置开始消费消息,非空时，例如TEST.0:1235917,TEST.1:99992,
    
    表示从TOPIC名字为TEST，PARTION分区标号0对应偏移地址是1235917,标号1对应99992...
  
er_rules=detail_table:parent_table:APPLY_NUM-APPLY_NUM:SERIAL_NUMBER:PLANS

    说明：detail_table表示子表,parent_table表示父表;APPLY_NUM-APPLY_NUM表示在主从表进行关联的字段;SERIAL_NUMBER表示出了APPLY_NUM关联字段外，
    
    另外一个可以唯一定位字表记录的字段；PLANS表示嵌入式DOCUMENT的名称
