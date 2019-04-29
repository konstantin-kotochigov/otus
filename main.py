import sys
from pyspark.sql import SparkSession
from hdfs import InsecureClient

import time
import datetime

from cj_loader import CJ_Loader
from cj_predictor import CJ_Predictor
from cj_export import CJ_Export



def main():
    
    if len(sys.argv) < 4:
        raise Exception("command must have 3 arguments")
    
    # Specifies to Merge
    send_update = True if sys.argv[1]=="send" else False
    
    # Overrides option to refit the model
    arg_refit = True if sys.argv[2]=="refit" else False
    
    # Sets sample rate
    arg_sample_rate = sys.argv[3]
    
    # send_update = True if len(sys.argv) >= 2 and (sys.argv[1]=="1") else False
    print("Send_update = {}".format(send_update))
    update_model_every = 60*24*7 # in seconds
    
    start_processing = time.time()
    
    # Common classes
    spark = SparkSession.builder.appName('analytical_attributes').getOrCreate()
    wd = "/user/kkotochigov/"
    hdfs_client = InsecureClient("http://159.69.59.101:50070", "hdfs")
    
    
    # Check whether We Need to Refit
    model_modification_ts = next(iter([x[1]['modificationTime'] for x in hdfs_client.list(wd+"models/", status=True) if x[0] == "model.pkl"]), None)
    model_needs_update = True if (model_modification_ts == None) or (time.time() - model_modification_ts > update_model_every) or (arg_refit) else False
    print("Refit = {}".format(model_needs_update))
    
    # Load Data
    cjp = CJ_Loader(spark)
    cjp.set_organization("21843d80-6f2c-402f-9587-9c501724c646")
    cjp.load_cj(ts_from=(2019,1,1), ts_to=(2019,1,31))
    # cjp.load_cj(ts_from=(2018,12,1), ts_to=(2018,12,31))
    # cjp.cj_stats(ts_from=(2010,12,1), ts_to=(2020,12,31))
    cjp.cj_data.createOrReplaceTempView('cj')
    cjp.extract_attributes()
    cjp.process_attributes(features_mode="seq", split_mode="all")
    data = cjp.cj_dataset
    
    # data.to_parquet(wd+"/data_export.parquet")
    
    # Sample Dataset to Reduce Processing Time
    # if arg_sample_rate != 1.0:
    #     (train_index, test_index) = StratifiedShuffleSplit(n_splits=1, train_size=arg_sample_rate).get_n_splits(data, data.target)
    
    # Make Model
    predictor = CJ_Predictor(wd+"models/", hdfs_client)
    predictor.set_data(data)
    predictor.optimize(batch_size=4096)
    
    start_fitting = time.time()
    result = predictor.fit(update_model=model_needs_update, batch_size=4096)
    
    scoring_distribution = result.return_score.value_counts(sort=False)
    
    print("Got Result Table with Rows = {}".format(result.shape[0]))
    print("Score Distribution = \n{}".format(scoring_distribution))
    
    
    
    # Make Delta
    df = spark.createDataFrame(result)
    dm = CJ_Export("21843d80-6f2c-402f-9587-9c501724c646", "model_update", "http://159.69.59.101:50070", "schema.avsc")
    
    mapping = {
        'id': {
            'fpc': {
                'primary': 10008,
                'secondary': 10031
            }
        },
        'attributes': {
            'return_score': {
                'primary': 10127,
                'mapping': {
                    '1': 10000,
                    '2': 10001,
                    '3': 10002,
                    '4': 10003,
                    '5': 10004
                }
            }
        }
    }
    
    # Publish Delta
    print("Send Update To Production = {}".format(send_update))
    dm.make_delta(df, mapping, send_update=send_update)
    
    finish_fitting = time.time()
    
    # Store Run Metadata
    log_data = [datetime.datetime.today().strftime('%Y-%m-%d %H-%m'),
        str(cjp.cj_data_rows),
        str(cjp.cj_df_rows),
        str(cjp.cj_dataset_rows),
        str(model_needs_update),
        str(send_update),
        str(round((start_fitting - start_processing)/60, 2)),
        str(round((finish_fitting - start_fitting)/60, 2)),
        str(predictor.train_auc),
        str(predictor.test_auc),
        str(predictor.test_auc_std),
        str(scoring_distribution[0]),
        str(scoring_distribution[1]),
        str(scoring_distribution[2]),
        str(scoring_distribution[3]),
        str(scoring_distribution[4])
    ]
    log = ";".join(log_data)
    
    log_path = wd+"log/log.csv"
    
    if "log.csv" not in hdfs_client.list(wd+"log/"):
        data_with_header = 'dt;loaded_rows;extracted_rows;processed_rows;refit;send_to_prod;processing_time;fitting_time;train_auc;test_auc;test_auc_std;q1;q2;q3;q4;q5\n'+log + "\n"
        hdfs_client.write(log_path, data=bytes(data_with_header, encoding='utf8'), overwrite=True)
    else:
            with hdfs_client.read(log_path) as reader:
                prev_log = reader.read()
            new_log = prev_log + bytes(log + "\n", encoding='utf8')
            hdfs_client.write(log_path, data=new_log, overwrite=True)
        



main()







        
