from os import truncate
import sys
from log_trans.util import get_token
from log_trans import log_proc


def kv_extract(line:str):
    pos_1 = line.find('String1')
    if pos_1 == -1:
        return ''

    tmp_convert_func = \
        lambda number, item : (number, item) if isinstance(item, str) else item


    log_header_str = line[:pos_1].strip(' ').strip('\t').strip('\n')
    log_string = line[pos_1:].strip(' ').strip('\t').strip('\n')

    header_kv_list = []
    # print(log_header_str)
    truncate_header_str = log_header_str
    while True:
        token, truncate_header_str = get_token(truncate_header_str)
        if len(header_kv_list) > 0 and header_kv_list[-1] == '=':
            header_kv_list.pop()
            key_str = header_kv_list.pop()
            kv_pair = (key_str, token)
            header_kv_list.append(kv_pair)
        else:
            header_kv_list.append(token)
        if (len(truncate_header_str)) <= 0:
            break
    header_kv_list[:] = \
        [tmp_convert_func(number, item) for number, item in enumerate(header_kv_list)]
    

    log_kv_list = []
    log_string = log_string.replace('String1="', '', 1).rstrip('"').strip(' ')
    # print(log_string)
    truncate_log_str = log_string
    while True:
        token, truncate_log_str = get_token(truncate_log_str)
        if len(log_kv_list) > 0 and log_kv_list[-1] == '=':
            log_kv_list.pop()
            key_str = log_kv_list.pop()
            kv_pair = (key_str, token)
            log_kv_list.append(kv_pair)
        else:
            log_kv_list.append(token)
        if (len(truncate_log_str)) <= 0:
            break
    log_kv_list[:] = \
        [tmp_convert_func(number, item) for number, item in enumerate(log_kv_list)]
    
    return header_kv_list, log_kv_list



def pre_filter(line:str):
    pass


if __name__ == "__main__":
    
    f = open('aux/aux_urp', 'r')
    line = f.readlines()[0]
    l1, l2 = kv_extract(line)
    print(l1)
    print(l2)

    # assert(len(sys.argv) > 2)

    # data_path = sys.argv[1]
    # config_path = sys.argv[2]

    # log_proc = log_proc.LogProcessor(data_path, kv_extract_func=kv_extract, pre_filter_func=pre_filter)

    # log_proc.generate_DF(config_path=config_path)

    # log_proc.generate_sql(table_name='urp_query_probe_log')

    # log_proc.execute_sql(database_name='hive')

    # log_proc.stop()
