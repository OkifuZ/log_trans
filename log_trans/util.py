from enum import Enum
from typing import Tuple


def post_kvlist_proc(vals:list) -> list :

    def camel_to_underscore(camel_format:str):

        if len(camel_format) <= 3:
            return camel_format.lower()

        underscore_format=''
        for _s_ in camel_format:
            underscore_format += _s_ if _s_.islower() else '_'+_s_.lower()
        return underscore_format.lstrip('_')

    def get_kv(kv_str:str):

        pos = kv_str.find(':')
        if pos != -1:
            return (camel_to_underscore(kv_str[:pos]), kv_str[pos+1:])

    return [get_kv(p) for p in vals]


def get_value(kv_list:list):
    return [tup[1] for tup in kv_list]


def expand_number_list(nlist):
    if len(nlist) == 0:
        return nlist
    expanded_list = []
    last_number = nlist[0]
    for cur_number in nlist[1:]:
        expanded_list += list(range(last_number, cur_number))
        last_number = cur_number
    expanded_list.append(last_number)
    return expanded_list


def load_config(conf_path:str):
    
    f = open(conf_path, 'r')
    
    lines = f.readlines()
    lines = list(map(lambda line: line.strip(), lines))
    lines = list(filter(lambda line: len(line) > 0 and line.find('#') != 0, lines))
    field_list = list(map(lambda line: line.split(' '), lines))

    # print(field_list)

    type_str_list = []
    select_str_list = []
    nullable_list = []
    for field in field_list:
        assert(len(field) >= 2)
        type_str_list.append(field[1])
        select_str_list.append(int(field[0]))
        bool_str = 'true' if len(field) == 2 else field[2]
        nullable_list.append(True if 'true' in bool_str.lower() else False)

    return type_str_list, select_str_list, nullable_list


def get_token(line:str) -> Tuple[str, str]:

    class ParseStatus(Enum):
        START = 1
        NORMAL_STR = 2
        PAIR_PEND = 3

    pair_pending_stack = []
    current_stat = ParseStatus.START

    next_token = ''
    end_pos = 0

    def is_map_char(c:str):
        return c == ':' or c =='='
    def is_assign_char(c:str):
        return c =='='
    def is_split_char(c:str):
        return c ==',' or c == ';'
    def is_pair_left(c:str):
        return c == '(' or c == '{' or c == '['
    def is_pair_right(c:str):
        return c == ')' or c =='}' or c == ']'
    def is_white(c:str):
        return c == ' ' or c == '\t' or c == '\n'
    def make_pair(c_left:str, c_right:str):
        pair_dict = {'(':')', '"':'"', '[':']', '{':'}'}
        return pair_dict[c_left] == c_right

    for i, c in enumerate(line):
        if current_stat is ParseStatus.START:
            if is_pair_left(c) or c == '"':
                pair_pending_stack.append(c)
                next_token += c
                current_stat = ParseStatus.PAIR_PEND
            elif is_split_char(c) or is_white(c):
                continue
            elif is_map_char(c):
                next_token += c
                end_pos = i
                break
            else:
                next_token += c
                current_stat = ParseStatus.NORMAL_STR
        elif current_stat is ParseStatus.NORMAL_STR:
            if is_split_char(c) or is_assign_char(c):
                end_pos = i-1
                break
            else:
                next_token += c
        elif current_stat is ParseStatus.PAIR_PEND:
            if c == '"' and \
                (len(pair_pending_stack) == 0 or pair_pending_stack[-1] != '"'):
                pair_pending_stack.append(c)
            elif c == '"' and \
                len(pair_pending_stack) != 0 and pair_pending_stack[-1] == '"':
                pair_pending_stack.pop()
            elif is_pair_left(c):
                pair_pending_stack.append(c)
            elif is_pair_right(c):
                c_ = pair_pending_stack.pop()

            next_token += c
            if len(pair_pending_stack) == 0:
                end_pos = i
                break
            
    # print(pair_pending_stack)    
    assert(len(pair_pending_stack) == 0)

    return next_token, line[end_pos+1:]

s = 'DB5AAP32811DFF2,1987600662,i,07/06/2021 00:03:11,QueryProbe,Urp CacheKey,Pid="23604" Tid="12716" TS="0x01D77234FBCD4C9A"'

while True:
    token, s = get_token(s)
    print(token)
    if len(s) == 0:
        break