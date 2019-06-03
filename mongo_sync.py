import sys
import time
import logging
import datetime
import os
from configparser import ConfigParser
import pymongo
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import BrokerMetadata, PartitionMetadata, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from collections import namedtuple
import json
import re


#outputDir='Data/Plans/'
#connection = pymysql.connect("192.168.177.138","root","root","flms" )
env_dist = os.environ
module_name='MONGO_SYNC'
etcPath = env_dist.get('PY_DIR')
if etcPath is None :
    etcPath = "db.cfg"
else:
    etcPath = env_dist.get('PY_DIR')+"/db.cfg"
cp = ConfigParser()
print(etcPath)
cp.read(etcPath)
idx =  cp.sections().index(module_name)
section = cp.sections()[idx]
logPath = cp.get(section, "log_path")
print(logPath)
mongo_url=cp.get(section, "mongo_url")
kafka_url=cp.get(section, "bootstrap_servers")
from_offset = None
if cp.has_option(section,'from_offset'):
    from_offset=cp.get(section, "from_offset")
else:
    print('NO from_offset in cfg')
special_tbs = cp.get(section, "special_tb") 
spe_table_list = special_tbs.split(',')
client = pymongo.MongoClient(mongo_url)
#logger.debug('[%d]Databases[%s]',iOdd,client.list_database_names())
DB_NAME = 'hscs'
db = client[ DB_NAME ]
shard_key_dict = {}
er_rules_dict = {}

run_date = time.strftime('%Y%m%d', time.localtime(time.time()))
logFile = logPath + "/"+module_name+"_"+run_date+".log"
'''
logging.basicConfig(level=logging.WARNING,
                    filename=logFile,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s'
                    )
logger = logging.getLogger(__name__)
'''
fh = logging.FileHandler(logFile,encoding='utf-8')
logger = logging.getLogger() 
logger.setLevel(logging.WARNING)
fm = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s')
logger.addHandler(fh)
fh.setFormatter(fm)

#print('Logger name='+logger.name)

def getShardKeys(shard_key_dict):
    db_cfg = client[ 'config' ]
    colls = db_cfg['collections']
    #results = invoice.find({'dropped':'false','_id':'/^hscs/'})
    results = colls.find({'dropped':False,'_id':{'$regex':'hscs'}})
    for res in results:
       shard_key_dict[res['_id']] = list(res['key'].keys())
    
    return 0

def transform_data(data_dict_list,sqltype_dict):
    for aDict in data_dict_list:
        for key, value in aDict.items():
            if sqltype_dict[ key ] in [-5,4]:
                #logger.warning('key:value=%s:%s',key,value)
                if value is not None:
                    aDict[ key ] = int(value)
            elif sqltype_dict[ key ] in [3]:
                if value is not None:
                    aDict[ key ] = float(value)
    return 0

def transform_pkdata(sTable,data_dict,pkname_list,pk_dict):
    aTable = dict_cdc['database']+'.'+sTable
    if pkname_list is None:
        logger.error('pkname_list is None')
        return 0
    for pk in pkname_list:
        pk_dict[ pk ] = data_dict[ pk ]
        if aTable in shard_key_dict:
            key_list = shard_key_dict[aTable]
            for i in range(len(key_list)):
                pk_dict[ key_list[i] ] = data_dict[ key_list[i] ]
    return 0

def deal_special_tb(dict_cdc):
    _mongo_db = client[dict_cdc['database']]
    _mongo_tb = _mongo_db[dict_cdc['table']]
    md_cfg_dict = er_rules_dict[ dict_cdc['table'] ]

    _parent_tb = _mongo_db[md_cfg_dict['parent']]
    
    parent_join_key = md_cfg_dict['parent_join_key']
    child_join_key = md_cfg_dict['child_join_key']
    child_sub_key = md_cfg_dict['child_sub_key']
    sub_docum = md_cfg_dict['sub_docum']
    #logger.debug('Enter in deal_special_tb[%s]',dict_cdc['table'])
    
    try:    
        if 'DELETE' == dict_cdc['type']:
            for aDict in dict_cdc['data']:
                logger.warning('%s,CONDITION=%s,%s',dict_cdc['type'],aDict[parent_join_key],aDict[child_sub_key])
                pk_dict = {}
                pk_dict[parent_join_key] = aDict[parent_join_key]
                tmp2_dict = {}
                tmp2_dict[child_sub_key] = aDict[child_sub_key]
                tmp_dict = {}
                tmp_dict[sub_docum] = tmp2_dict
                op_dict = {}
                op_dict['$pull'] = tmp_dict
                logger.warning('%s,CONDITION=%s,%s',dict_cdc['type'],pk_dict,op_dict)
                #xxxx.update({APPLY_NUM:394414},{ $pull: {PLANS:{ SERIAL_NUMBER:98 } } },
                result = _parent_tb.update_one(pk_dict,op_dict)
                logger.warning('DELETE==matched_count[%d],modified_count[%d],upserted_id[%s]',
            result.matched_count,result.modified_count,result.upserted_id )
            
        elif dict_cdc['type'] in  ['UPDATE','INSERT']:
                #{ $addToSet: { tags: { $each: [ "camera", "electronics", "accessories" ] } } }
                #先删除,后addToSet
            for aDict in dict_cdc['data']:
                logger.warning('%s,CONDITION=%s,%s',dict_cdc['type'],aDict[parent_join_key],aDict[child_sub_key])
                pk_dict = {}
                pk_dict[parent_join_key] = aDict[parent_join_key]
                tmp2_dict = {}
                tmp2_dict[child_sub_key] = aDict[child_sub_key]
                tmp_dict = {}
                tmp_dict['PLANS'] = tmp2_dict
                op_dict = {}
                op_dict['$pull'] = tmp_dict
                logger.warning('%s,CONDITION=%s,%s',dict_cdc['type'],pk_dict,op_dict)
                #xxxx.update({APPLY_NUM:394414},{ $pull: {PLANS:{ SERIAL_NUMBER:98 } } },
                result = _parent_tb.update_one(pk_dict,op_dict)
                logger.warning('DELETE==matched_count[%d],modified_count[%d],upserted_id[%s]',
                result.matched_count,result.modified_count,result.upserted_id )
                new_dict = {}
                new_dict['PLANS'] = tmp2_dict
                insert_op_dict = {}
                insert_op_dict['$push'] = new_dict
                #logger.warning('%s,CONDITION=%s,%s',dict_cdc['type'],pk_dict,insert_op_dict)
                #xxxx.update({APPLY_NUM:394414},{ $push: {PLANS:{ XXXX } } },
                result = _parent_tb.update_one(pk_dict,insert_op_dict,upsert=True)
                logger.warning('push==matched_count[%d],modified_count[%d],upserted_id[%s]',
                    result.matched_count,result.modified_count,result.upserted_id )
                if result.modified_count == 0:
                    return -1
    except Exception as exc:
        logger.error('Exception[%s]=%s',type(exc),exc)
        return -1
    return 0

def dealCDCData(dict_cdc):
    _mongo_db = client[dict_cdc['database']]
    '''
    if dict_cdc['table'] not in _mongo_db.list_collection_names():
       result = _mongo_db.create_collection(dict_cdc['table']) 
       logger.warning('create_collection[%s]:[%s]',dict_cdc['table'],result) 
    '''
    _mongo_tb = _mongo_db[dict_cdc['table']]
    
    if dict_cdc['data'] is not None:
        row_num = len(dict_cdc['data'])
        transform_data(dict_cdc['data'],dict_cdc['sqlType'])
        logger.warning('Type=%s,Table=%s,rowcount=%d',dict_cdc['type'],dict_cdc['table'],row_num)
    
    #pk_shard = {}
    #sTable = dict_cdc['database']+'.'+dict_cdc['table']
    sTable = dict_cdc['table']
#    logger.warning('%s table=%s',dict_cdc['type'] ,sTable)
    
    if sTable in er_rules_dict:
         return deal_special_tb(dict_cdc)

    if dict_cdc['type'] in  ['UPDATE','INSERT'] :
        try:
            for aDict in dict_cdc['data']:    
                pk_dict = {}
                transform_pkdata(sTable,aDict,dict_cdc['pkNames'],pk_dict)
                logger.warning('UPDATE,pk=%s',pk_dict)
                op_dict = {}
                op_dict['$set'] = aDict 
                logger.warning('UPDATE,COLUMN=%s',op_dict)
                result = _mongo_tb.update_one(pk_dict,op_dict,upsert=True)
                logger.warning('%s result,matched_count[%d],modified_count[%d],upserted_id[%s],pk[%s]',
            dict_cdc['type'],result.matched_count,result.modified_count,result.upserted_id,pk_dict )
        except Exception as exc:
            logger.error('Exception[%s]=%s',type(exc),exc)
            return -1
    
    elif dict_cdc['type'] == 'DELETE' :
        try:
            for aDict in dict_cdc['data']: 
                pk_dict = {}
                transform_pkdata(sTable,aDict,dict_cdc['pkNames'],pk_dict)
                logger.warning('DELETE,pk=%s',pk_dict)
                result = _mongo_tb.delete_one(pk_dict)    
                logger.warning('DELETE,affected=%d',result.deleted_count)
        except Exception as exc:
            logger.error('Exception[%s]=%s',type(exc),exc)
            return -1

    elif dict_cdc['type'] == 'CREATE' :
        try:
            _mongo_tb = _mongo_db.create_collection(dict_cdc['table'])
            #获得PRIMARY KEY信息        
            str_ddl = dict_cdc['sql']
            regex = r"PRIMARY\s+KEY\s*\((\S+)\)"
            result = re.search(regex, str_ddl,  re.MULTILINE)
            logger.warning('CREATE pk[%s]=%s',type(result),result)
            pk_list = []
            for aKey in result.group(1).replace('"','').split(','):
                aTuple = ( aKey,pymongo.ASCENDING )
                pk_list.append(aTuple) 
            logger.warning('PK LIST=%s',pk_list)   
            _mongo_tb.create_index(pk_list)
        except Exception as exc:
            logger.warning('Exception=%s',exc)
            #if isinstance(exc, pymongo.errors.CollectionInvalid):
            return -1

    elif dict_cdc['type'] == 'ERASE' :
        try:
            result =  _mongo_db.drop_collection(dict_cdc['table'])
            logger.warning('drop_collection [%s]=%d',type(result),result['ok'])
        except Exception as exc:
            logger.warning('Exception=%s',exc)
            #if isinstance(exc, pymongo.errors.CollectionInvalid):
            return -1
        return result['ok']
    else :
        logger.warning(dict_cdc['type'])

    return 0

def get_er_rules(er_rules_dict):
    cp = ConfigParser()
    cp.read(etcPath)
    idx =  cp.sections().index('MONGO_SYNC')
    section = cp.sections()[idx]
    str_er_rule = cp.get(section, "er_rules")
    #print(str_er_rule)
    for str in str_er_rule.split(';'):
        if(len(str)==0):
            break
        rules_dict = {}
        _cfg_list = str.split(':')
        #print(_cfg_list)
        if len(_cfg_list) != 5:
            return -1
        cfg_dict = {}
        cfg_dict['parent'] = _cfg_list[1]
        cfg_dict['parent_join_key'] = _cfg_list[2].split('-')[0]
        cfg_dict['child_join_key'] = _cfg_list[2].split('-')[1]
        cfg_dict['child_sub_key'] = _cfg_list[3]
        cfg_dict['sub_docum'] = _cfg_list[4]
        er_rules_dict[_cfg_list[0]] = cfg_dict
    
    logger.warning('er_rules_list:%s',er_rules_dict)    
    return 0


if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) != 3:
        print('Usage:{0} <Topic> <Group>'.format(sys.argv[0]) )
        sys.exit(-1)
   
    Topic = sys.argv[1]
    Group = sys.argv[2]
    logger.warning('Begin to run...')
    #06/03 获得ER rules
    get_er_rules(er_rules_dict)
    #exit(0)
    
    getShardKeys(shard_key_dict)
    logger.warning('shard_key_dict=%s',shard_key_dict)

    consumer = KafkaConsumer(bootstrap_servers= kafka_url,
        group_id=Group,
        enable_auto_commit=False,
        auto_offset_reset='latest',
        api_version=(0,10)
    )
    
    logger.warning('Topic[%s],sub[%s],Group[%s]',consumer.topics(),consumer.subscription(),Group )
    #返回SET{}
    ppp = consumer.partitions_for_topic(Topic)
    if ppp is None:
        logger.error('TOPIC[%s] not exist in cluster',Topic)
        admin_client = KafkaAdminClient(bootstrap_servers= kafka_url)
        topic_list = []
        topic_list.append(NewTopic(name=Topic, num_partitions=1, replication_factor=1))
        result = admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.warning('create_topics[%s]=%s',type(result),result)
        #exit(-1)    
    
    #tps,list中每个是TopicPartition(nameturple)
    tps = [TopicPartition(Topic, p) for p in ppp]
    for tp in tps:
        logger.warning('PS[%s:%d]',tp.topic,tp.partition)
    
    #返回dict{TopicPartition: int}
    latest_offset = consumer.end_offsets(tps)
    for tp in tps:
        logger.warning('LatestOffset[%s:%d]=%d',tp.topic,tp.partition,latest_offset[tp])
    #for _offset in consumer.end_offsets(tps):
    #    logger.warning('Offset[%s]=%d',tp.topic,tp.partition)

    #logger.warning('latest Offset[%s]=%s',type(latest_offset),latest_offset)
    consumer.assign(tps)
    #TopicPartition = namedtuple("TopicPartition", ["topic", "partition"])
    #partition_offset = []

    if from_offset is None:
        for i in range(len(tps)):
            commit_offset = consumer.committed(tps[i])
            if commit_offset is not None:
                logger.warning('Topic[%s:%d] commit Offset=%d',tp.topic,tp.partition,commit_offset)
                consumer.seek(tps[i], commit_offset)
    else:
        a_dict = {}
        for aStr in from_offset.split(','):
            key=aStr.split(':')[0]
            value=int(aStr.split(':')[1])
            a_dict[key]=value   
        logger.warning('Initial Offset =%s',a_dict)
        for i in range(len(tps)):
            aTP = tp.topic+'.'+str(tp.partition)
            logger.warning('aTP=%s',aTP)
            consumer.seek(tps[i],  a_dict[aTP])    
        #partition_offset.append(commit_offset)  

    '''
    for i in range(len(tps)):
        logger.warning('consumer.position=%d',consumer.position(tps[i]))
    '''
    
    
    iCnt = 0
    for msg in consumer:
        dict_cdc = json.loads(msg.value)
        logger.warning("%s:%d:%d: key=%s,value=%s" % (msg.topic, msg.partition,
                                          msg.offset, msg.key,dict_cdc))
        if 0 == dealCDCData(dict_cdc):
        #iCnt += 1
            consumer.commit()
        else:
            break
        #if iCnt == 10:
        #    break
    
    
    logger.warning('DEMO COMPLETE')
