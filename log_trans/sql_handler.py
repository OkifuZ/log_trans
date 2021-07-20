def sql_create_table(table_name:str, field_name_list:list, field_typestr_list:list):

    field_num = len(field_name_list)
    
    if len(field_typestr_list) != field_num:
        raise Exception()

    create_sql = 'create table if not exists ' + table_name
    field_str_sql = '('
    for i in range(field_num):
        name_type = field_name_list[i] + ' ' + field_typestr_list[i]
        name_type = name_type + (', ' if i is not field_num-1 else ')')
        field_str_sql = field_str_sql + name_type
    create_sql = create_sql + field_str_sql

    return create_sql


def sql_insert_table(tab_name_src: str, tab_name_dst:str):
    return 'insert into ' + tab_name_dst + ' select * from ' + tab_name_src

def sql_select(tab_name: str, condition_list):
    pass

