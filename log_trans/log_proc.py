
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import Row
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import FloatType

import log_trans.util as ut
import log_trans.sql_handler as tb

class LogProcessor:

    def __init__(self, log_path:str, kv_extract_func, pre_filter_func=lambda _:True) -> None:

        """
        Init LogProcessor, a logProcessor instant only serve for one log but multiple tables

        Parameters
        ----------
        log_path : str
            path to logs

            - for hdfs, simply use bare absolute path like '/user/hive/logs/log.txt'
            
            - for local file, path shoud be prefixed with 'file://', like 'file:///usr/home/user/logs/log.txt'
        
        kv_extract_func : function

            User defined function: (str) -> list[str]
        
            take a line of log as input, transform it to a list contains string like 'key_name:value' 
        
        pre_filter_func : function

            User defined function: (str) -> boolean

            take a line of log as input, decide whether to use it

        """

        # log
        self.__log_path = log_path

        # conf
        self.__fieldStr_list = []
        self.__nullable_list = []
        self.__fieldName_list = []
        self.__selected_field = []

        # basic rdd
        self.__spark = None
        self.__kv_pairs_rdd = None
        self.__first_kv_list = None
        self.__values_rdd = None

        # data frame
        self.__log_DF = None

        # sql
        self.create_table_sql = ''
        self.insert_table_sql = ''

        # aux function
        self.__kv_extract_function = kv_extract_func
        self.__pre_filter_func = pre_filter_func

        # stop
        self.__available = True

        self.__init_spark()
        self.__read_basic()


    def __init_spark(self) -> None:
        self.__spark = SparkSession\
        .builder\
        .enableHiveSupport()\
        .getOrCreate()
        print('INIT saprk session')

    
    
    def __read_basic(self) -> None:
        try:
            aux_kv_extract_function = self.__kv_extract_function
            aux_pre_filter_function = self.__pre_filter_func

            log_path = self.__log_path
            lines_raw = self.__spark.read.text(log_path).rdd.map(lambda r: r["value"])

            lines = lines_raw.filter(f=aux_pre_filter_function)

            self.__kv_pairs_rdd = lines.map(f=aux_kv_extract_function).\
                                        map(f=ut.post_kvlist_proc).persist()
            self.__values_rdd = self.__kv_pairs_rdd.map(ut.get_value).persist()
            self.__first_kv_list = self.__kv_pairs_rdd.first()
        except:
            self.stop()
            raise
    
    
    def __create_schema(self, fieldTypeStr_list, fieldName_list, nullable_list=[]):

        field_num = len(fieldName_list)

        fieldType_list = []
        field_list = []

        for _, type_str  in enumerate(fieldTypeStr_list):
            up_str: str = type_str.upper()
            if up_str == 'STRING':
                fieldType_list.append(StringType())
            elif up_str == 'INTEGER' or up_str == 'INT':
                fieldType_list.append(IntegerType())
            elif up_str == 'BOOLEAN':
                fieldType_list.append(BooleanType())
            elif up_str == 'FLOAT':
                fieldType_list.append(FloatType())
            else:
                pass

        # print(fieldType_list)
        for i in range(field_num):
            # print(i)
            field = StructField(name=fieldName_list[i], 
                                dataType=fieldType_list[i], 
                                nullable=nullable_list[i] 
                                    if len(nullable_list) != 0 else True)
            field_list.append(field)
        

        schema = StructType(field_list)

        return schema


    def generate_DF(self, config_path:str) -> DataFrame:
        """
        Generate dataframe for log data

        Parameters
        ----------
        config_path : path to config_file
        """
        if not self.__available:
            print('session not available')
            return None

        try:

            field_typestr_list, select_field, nullable_list = ut.load_config(config_path)
            self.__fieldStr_list = field_typestr_list
            self.__nullable_list = nullable_list
            self.__selected_field = select_field

            assert(len(field_typestr_list) == len(select_field))
            assert(len(nullable_list) == 0 or len(select_field) == len(nullable_list))

            # --- begin spark udfs
            def type_handler(value_list) -> list:
                # Type convert 
                # pass to spark transformation
                new_value_list = []
                for type_str, value in zip(field_typestr_list, value_list):
                    type_str_up = type_str.upper()
                    if type_str_up == 'INTEGER' or type_str_up == 'INT':
                        new_value_list.append(int(value))
                    elif type_str_up == 'FLOAT':
                        new_value_list.append(float(value))
                    elif type_str_up == 'BOOLEAN':
                        value_str_low = value.lower()
                        new_value_list.append(True if 'true' in value_str_low else False)
                    else:
                        new_value_list.append(value)
                return new_value_list
            
            select_field_expand = self.__selected_field

            def filter_selected_field(fields:list):
                # filter selected fields 
                # pass to spark transformation
                selected_field = []
                for field_id in select_field_expand:
                    assert(field_id > 0)
                    selected_field.append(fields[field_id-1])
                return selected_field
            # --- end spark udfs
            

            rows_rdd = self.__values_rdd.\
                map(filter_selected_field).\
                    map(type_handler).\
                        map(lambda values: Row(*tuple(values)))

            self.__fieldName_list = [kv_pair[0] for kv_pair in filter_selected_field(self.__first_kv_list)]
            schema = self.__create_schema(self.__fieldStr_list, self.__fieldName_list, self.__nullable_list)
            self.__log_DF = self.__spark.createDataFrame(data=rows_rdd, schema=schema)
            return self.__log_DF
        except:
            self.stop()
            raise


    def generate_sql(self, table_name:str) -> None:
        """
        Generate sql for creating table and loading data

        Parameters
        ----------
        table_name : str
        """
        if not self.__available:
            return

        try:
            tab_name = table_name
            self.create_table_sql = tb.sql_create_table(tab_name, self.__fieldName_list, self.__fieldStr_list)
            self.insert_table_sql = tb.sql_insert_table('temp_'+tab_name, tab_name)
        except:
            self.stop()
            raise


    def execute_sql(self, database_name:str) -> None:
        """
        execute sql generated by generate_sql

        Parameters
        ----------
        database_name : str

            database you wanna use
        """
        if not self.__available:
            print('session not available')
            return

        try:
            self.__spark.sql('use ' + database_name)
            self.__spark.sql(self.create_table_sql)
            self.__spark.sql(self.insert_table_sql)
        except:
            self.stop()
            raise


    def clear(self) -> None:
        """
        prepare for creating another table from the same log

        this method should appear after one table is fully created
        """
        if not self.__available:
            print('session not available')
            return
        # conf
        self.__fieldStr_list = []
        self.__nullable_list = []
        self.__fieldName_list = []

        # data frame
        self.__log_DF = None

        # sql
        self.create_table_sql = ''
        self.insert_table_sql = ''

    
    def stop(self) -> None:
        '''
        this method should appear at the end of app
        '''
        self.__spark.stop()
        self.clear()
        print('STOP saprk session')

