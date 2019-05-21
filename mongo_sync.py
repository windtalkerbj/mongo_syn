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
client = pymongo.MongoClient(mongo_url)
#logger.debug('[%d]Databases[%s]',iOdd,client.list_database_names())
DB_NAME = 'hscs'
db = client[ DB_NAME ]
shard_key_dict = {}

run_date = time.strftime('%Y%m%d', time.localtime(time.time()))
logFile = logPath + "/"+module_name+"_"+run_date+".log"

logging.basicConfig(level=logging.WARNING,
                    filename=logFile,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s')
logger = logging.getLogger(__name__)
#print('Logger name='+logger.name)

def getShardKeys(shard_key_dict):
    db_cfg = client[ 'config' ]
    colls = db_cfg['collections']
    #results = invoice.find({'dropped':'false','_id':'/^hscs/'})
    results = colls.find({'dropped':False,'_id':{'$regex':'hscs'}})
    for res in results:
       shard_key_dict[res['_id']] = list(res['key'].keys())
    
    return 0

def dealCDCData(dict_cdc):
    '''
    if dict_cdc['isDdl'] == True :
        return 0
    '''
    _mongo_db = client[dict_cdc['database']]
    '''
    if dict_cdc['table'] not in _mongo_db.list_collection_names():
       result = _mongo_db.create_collection(dict_cdc['table']) 
       logger.warning('create_collection[%s]:[%s]',dict_cdc['table'],result) 
    '''
    _mongo_tb = _mongo_db[dict_cdc['table']]
    if dict_cdc['data'] is not None:
        row_num = len(dict_cdc['data'])
        logger.warning('Type=%s,Table=%s,rowcount=%d',dict_cdc['type'],dict_cdc['table'],row_num)
    
    if dict_cdc['type'] == 'UPDATE' :
        sTable = dict_cdc['database']+'.'+dict_cdc['table']
        logger.warning('UPDATE table=%s',sTable)
        for aDict in dict_cdc['data']:    
            pk_dict = {}
            for pk in dict_cdc['pkNames']:
                pk_dict[ pk ] = aDict[ pk ]
            
            #如果该表是SHARD，加上SHARD KEY
            if sTable in shard_key_dict.keys():
                key_list = shard_key_dict[sTable]
                for i in range(len(key_list)):
                    pk_dict[ key_list[i] ] = aDict[ key_list[i] ]    
            
            logger.warning('UPDATE,pk=%s',pk_dict)
            op_dict = {}
            op_dict['$set'] = aDict 
            logger.warning('UPDATE,COLUMN=%s',op_dict)
            result = _mongo_tb.update_one(pk_dict,op_dict,upsert=True)
            logger.warning('update result,matched_count[%d],modified_count[%d],upserted_id[%s]',
        result.matched_count,result.modified_count,result.upserted_id )
        #if result.matched_count == 0 :
        #     result = _mongo_tb.insert_one(pk_dict,op_dict,upsert=False) 
        
    elif dict_cdc['type'] == 'INSERT' :
        for aDict in dict_cdc['data']: 
            result = _mongo_tb.insert_one(aDict)
            pkVals = {}
            for aKey in  dict_cdc['pkNames']: 
                pkVals[aKey] = aDict[aKey]
            logger.warning('INSERT result id=%s,pk=%s',result.inserted_id,pkVals)
    elif dict_cdc['type'] == 'DELETE' :
        #logger.warning('DELETE')
        for aDict in dict_cdc['data']: 
            pk_dict = {}
            for pk in dict_cdc['pkNames']:
                pk_dict[ pk ] = aDict[ pk ]
            result = _mongo_tb.delete_one(pk_dict)    
            logger.warning('DELETE,affected=%d',result.deleted_count)
    elif dict_cdc['type'] == 'CREATE' :
        logger.warning('Type==Create,[%s]',dict_cdc['table'])
        try:
            _mongo_tb = _mongo_db.create_collection(dict_cdc['table'])
        except Exception as exc:
            logger.warning('Exception=%s',exc)
            if isinstance(exc, pymongo.errors.CollectionInvalid):
                return 0
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
    elif dict_cdc['type'] == 'ERASE' :
        logger.warning('Type==DROP [%s]',dict_cdc['table'])
        result =  _mongo_db.drop_collection(dict_cdc['table'])
        logger.warning('drop_collection [%s]=%d',type(result),result['ok'])
        return result['ok']
    else :
        logger.warning(dict_cdc['type'])

    return 0

def demo_to_plans():
    line = db['hscs_itf_imp_lines']
    condition = {
    'H_PROCESS_STATUS':'S',
    'H_DISABLE_FLAG':'N',
    'PROCESS_STATUS':'S',
    'IMPORT_STATUS':{'$ne':'S'},
    'INTERFACE_NAME':{ '$in' :['ALIX_AR_INTERFACE','OPL_AR_INTERFACE']}
    }
    startTime = time.perf_counter()
    results = line.find(condition,no_cursor_timeout = True).sort( [
        ('VALUE2', pymongo.ASCENDING),
        ('LINE_ID', pymongo.DESCENDING)])
   
    #list_result = []
    iCnt = 0
    iMiss = 0
    for res in results:
        iCnt +=1 
        tmp_dict = dict({'INTERFACE_NAME': res['INTERFACE_NAME'], 'LINE_ID': res['LINE_ID'],\
                'VALUE2':res['VALUE2'],'VALUE3':res.get('VALUE3', 'not_exist'),'VALUE4':res['VALUE4'],'VALUE5':res['VALUE5'],'VALUE6':res['VALUE6'],\
        'VALUE7':res['VALUE7'],'VALUE8':res['VALUE8'],'VALUE9':res['VALUE9'],'VALUE10':res['VALUE10'],'VALUE11':res['VALUE11']
        })
                #list_result.append(tmp_dict)
        if iCnt % 100 == 0 :
            logger.debug('(%d)RESULT DEALOK',iCnt) 
       
    endTime = time.perf_counter() 
    logger.debug('Hit:%d,Miss:%d (%f SECs)',iCnt,iMiss,(endTime-startTime))
    #CHECK APPLY_NO
    return 0

if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) != 3:
        print('Usage:{0} <Topic> <Group>'.format(sys.argv[0]) )
        sys.exit(-1)
   
    Topic = sys.argv[1]
    Group = sys.argv[2]
    logger.warning('Begin to run...')
    
    
    getShardKeys(shard_key_dict)
    logger.warning('shard_key_dict=%s',shard_key_dict)

    consumer = KafkaConsumer(bootstrap_servers= kafka_url,
        group_id=Group,
        enable_auto_commit=False,
        auto_offset_reset='latest',
        api_version=(0,10)
    )
    
    logger.warning('Topic[%s],sub[%s],Group[%s]',consumer.topics(),consumer.subscription(),Group )
    tps = [TopicPartition(Topic, p) for p in consumer.partitions_for_topic(Topic)]
    for tp in tps:
        logger.warning('PS[%s:%d]',tp.topic,tp.partition)
    
    latest_offset = consumer.end_offsets(tps)
    for tp in tps:
        logger.warning('LatestOffset[%s:%d]=%d',tp.topic,tp.partition,latest_offset[tp])
    consumer.assign(tps)

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

    iCnt = 0
    for msg in consumer:
        dict_cdc = json.loads(msg.value)
        logger.warning("%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition,
                                          msg.offset, msg.key,dict_cdc))
        dealCDCData(dict_cdc)
        #iCnt += 1
        consumer.commit()
        #if iCnt == 10:
        #    break
    
    
    logger.warning('DEMO COMPLETE')
