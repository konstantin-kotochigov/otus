import pandas
import numpy
import time
import datetime
import re

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import SparkSession

class CJ_Loader:
    
    cj_path = ""
    cp_path = ""
    
    cj_data = None
    cj_attributes = None
    cj_dataset = None
    cj_df = None
    
    cj_data_rows = None
    cj_df_rows = None
    cj_dataset_rows = None
    
    def __init__(self, spark):
        self.cj_path = ""
        self.cp_path = ""
        self.spark = spark
        self.spark.udf.register("cj_id", self.cj_id, ArrayType(StringType()))
        self.spark.udf.register("cj_attr", self.cj_attr, ArrayType(StringType()))
        
    def set_organization(self, org_uid="57efd33d-aaa5-409d-89ce-ff29a86d78a5"):
        self.cj_path = "/data/{}/.dmpkit/customer-journey/master/cdm".format(org_uid)
        self.cp_path = "/data/{}/.dmpkit/profiles/master/cdm".format(org_uid)
        print("Setting CJ Data Path To: {}".format(self.cj_path))
        print("Setting CP Data Path To: {}".format(self.cj_path))
    
    def load_cj_all(self):
        self.cj_data = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        self.cj_data_rows = self.cj_data.count()
        print("Loaded CJ Rows (Full) = {}".format(self.cj_data_rows))
    
    def load_cj(self, ts_from, ts_to):
        cj_all = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        time_from = int(time.mktime(datetime.datetime(ts_from[0],ts_from[1],ts_from[2]).timetuple())) * 1000
        time_to = int(time.mktime(datetime.datetime(ts_to[0], ts_to[1], ts_to[2]).timetuple())) * 1000
        self.cj_data = cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
        self.cj_data_rows = self.cj_data.count()
        print("Loaded CJ Rows = {}".format(self.cj_data_rows))
    
    def cj_stats(self, ts_from=(2000,1,1), ts_to=(2100,1,1)):
        cj_all = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        time_from = int(time.mktime(datetime.datetime(ts_from[0],ts_from[1],ts_from[2]).timetuple())) * 1000
        time_to = int(time.mktime(datetime.datetime(ts_to[0], ts_to[1], ts_to[2]).timetuple())) * 1000
        cj_all = cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
        cj_all.selectExpr("date(from_unixtime(ts/1000)) as ts").groupBy("ts").count().orderBy("ts").show(100)
        cj_all.selectExpr("date(from_unixtime(min(ts/1000))) as min_ts","date(from_unixtime(max(ts/1000))) as max_ts","count(*) as cnt").show()
    
    @staticmethod
    def cj_id(cj_ids, arg_id, arg_key=-1):
        result = []
        for id in cj_ids['uids']:
            if id['id'] == arg_id and id['key'] == arg_key:
                result += [id['value']]
        return result
    
    @staticmethod
    def cj_attr(cj_attributes, arg_id, arg_key=None):
        result = []
        if cj_attributes is not None:
            for attr in cj_attributes:
                for member_id in range(0, 8):
                    member_name = 'member' + str(member_id)
                    if attr is not None and member_name in attr:
                        if attr[member_name] is not None and 'id' in attr[member_name]:
                            if attr[member_name]['id'] == arg_id and ('key' not in attr[member_name] or attr[member_name]['key'] == arg_key):
                                result += [attr[member_name]['value']]
        return result
        
    
    # Method to get attributes (returns dataframe)
    def extract_attributes(self):
        
        # Link processing function
        def __get_link(raw_link):
            return (substr(raw_link, 16, substring_index(raw_link, '?', 1)))
            
        def __get_link(raw_link):
            return raw_link.find("?")
        
        # Select CJ Attributes
        cj_df = self.spark.sql('''
            select
                cj_id(id, 10008, 10031)[0] as fpc,
                substring(substring_index(cj_attr(attributes, 10071)[0], '?', 1), 1, 100) as link,
                ts/1000 as ts
            from cj c
        ''').filter("link is not null and fpc is not null")
        
        # Compute TS deltas between events (in hours)
        cj_df.createOrReplaceTempView("cj_df")
        cj_df_attrs = self.spark.sql("select fpc, link, ts, lead(ts) over (partition by fpc order by ts) as next_ts from cj_df")
        cj_df_attrs = cj_df_attrs.withColumn("next",(cj_df_attrs["next_ts"]-cj_df_attrs["ts"]) / 3600)
        cj_df_attrs.createOrReplaceTempView("cj_df_attrs")
        self.cj_df = cj_df_attrs.select("fpc","ts","next","link")
        
        self.cj_df_rows = self.cj_df.count()
        print("Extracted Rows (cj_df) = {}".format(self.cj_df_rows))
        
        return
    
    
    def process_attributes(self, return_window=30, features_mode='seq', split_mode='all', split_dt=None):
    
        sessions_upper_bound = time.time() - return_window*24*60*60
        
        print("Using Return Window = {} days".format(return_window))
        print("Right Bound = {}".format(datetime.datetime.fromtimestamp(sessions_upper_bound).strftime("%Y-%m-%d")))
        
        
        # Generate Target Variables for Each CJ
        def compute_target(event_list, target_url_regexp, lookahead):
            
            # Set initial next target TS
            next_target_ts = 10000000000
            
            # Construct Target Variables List
            target_list = [-1] * len(event_list[0])
            
            for t in range(len(event_list[0])-1,-1,-1):
        
                # Set Target variable
                target_list[t] = 0 if next_target_ts - event_list[0][t] > lookahead else 1
                
                # If we encounter Target Url, Update Next Target TS
                if re.match(target_url_regexp, event_list[2][t]):
                    next_target_ts=event_list[0][t]
            
            return target_list
            
        
        def process_sequence1(id, event_sequence, session_close_event_num, targets):
            return (
                id,                                                               # FPC
                event_sequence[1][0:session_close_event_num+1],                   # deltas
                event_sequence[2][0:session_close_event_num+1],                   # urls
                targets[session_close_event_num],                                  # Target
                event_sequence[0][session_close_event_num]                        # TS
            )
        
        # Function to convert sequence of events into feature vector
        def process_sequence2(id, event_sequence, session_close_event_num):
            f1 = numpy.avg(event_sequence[0:session_close_event_num+1])
            f2 = numpy.min(event_sequence[0:session_close_event_num+1])
            f3 = f4 = f5 = 0
            return (id, f1, f2, f3, f4, f5)
    
        # Create Multiple Session Records
        def process_event_list(spark_grouped_row, features_mode, split_mode):
        
            customer_id = spark_grouped_row[0]
            data_list = spark_grouped_row[1]
            
            # Sort events by timestamp
            data_list = sorted(data_list, key=lambda y: y[0])
            
            # Divide event attributes into separate lists
            event_lists = list(zip(*data_list))
            deltas = event_lists[1]
            timestamps = event_lists[0]
            
            # Generate Targets
            targets = compute_target(event_lists, "^https://otus.ru/assessment/", lookahead=60*24*60*60)
            
            # Choose Session boundaries
            if split_mode == "all":
                # We seek large deltas and mark those points as session ends
                session_coordinates = [i for i, x in enumerate(deltas) if x == None or x > 4]
            else:
                # We got one splitting point and generate one row preceding this TS
                session_coordinates = [max([i for i,x in enumerate(timestamps) if x < split_dt])]
                
            # Choose a Function For Feature Generation
            if features_mode == "seq":
                process_function = process_sequence1
            else:
                process_function = process_sequence2
            
            return [process_function(customer_id, event_lists, session_close_event_num, targets) for session_close_event_num in session_coordinates]
            
        
        # Slice By User
        y = self.\
            cj_df.\
            select(['fpc','ts','next','link']).rdd.map(lambda x: (x['fpc'], (x['ts'], x['next'], x['link']))).\
            groupByKey().\
            flatMap(lambda x: process_event_list(x, features_mode, split_mode)).filter(lambda x: x[4] < sessions_upper_bound).map(lambda x: x[0:4])
        
        # Convert To Pandas dataframe
        y_py = pandas.DataFrame(y.collect(),  columns=['fpc','dt','url','target'])
        
        # Process Types
        y_py['url'] = y_py.url.apply(lambda x:" ".join(x))
        y_py['dt'] = y_py.dt.apply(lambda x:[y for y in x if y != None])
        
        self.cj_dataset = y_py
        
        self.cj_dataset_rows = self.cj_dataset.shape[0]
        print("Dataset of Processed Rows (cj_dataset) = {}".format(self.cj_dataset_rows))
        
        return