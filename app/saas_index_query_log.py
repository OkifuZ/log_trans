import sys
from log_trans.log_proc import LogProcessor

'''
    `set hive.resultset.use.unique.column.names=false;`
    Usage: Avoid printing table name in column name while using hive cli
'''

# udf
def kv_extract(line:str):
    line = line.strip().replace('String1="', '').rstrip('"')

    line = line.replace('Query Log', 'QueryLog').\
        replace('Rest Match', 'RestMatch').\
            replace('Documents Matched', 'DocumentsMatched').\
                replace('UrlHashes Matched', 'UrlHashesMatched').\
                    replace('=', ':').\
                        replace('TraceID finished', 'TraceIdFinished')

    line = line.replace(' ', '')
    vals = line.split(',')
    iden = vals[4]
    iden = iden.replace('Tid', ',Tid').\
        replace('TS', ',TS').\
            replace('Trace', ',Trace').\
                replace('"', '')
    idens = iden.split(',')
    vals = vals[:4] + idens + vals[5:]
    
    vals[0] = 'Alpha:' + vals[0]
    vals[1] = 'EstablishTime:' + vals[1]
    vals[2] = 'LogType:' + vals[2]

    return vals

# udf
def pre_filter(line:str):
    return 'QueryTotalLatency' in line


if __name__ == "__main__":

    assert(len(sys.argv) > 2)
    
    data_path = sys.argv[1]
    config_path = sys.argv[2]
    

    log_proc = LogProcessor(data_path, kv_extract_func=kv_extract, pre_filter_func=pre_filter)

    log_proc.generate_DF(config_path=config_path)

    log_proc.generate_sql(table_name='index_query_log')

    log_proc.execute_sql(database_name='hive')

    log_proc.stop()

