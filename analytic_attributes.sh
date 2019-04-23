export HADOOP_USER_NAME=dmpkit
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
PYSPARK_PYTHON=./venv/bin/python spark-submit --master yarn --deploy-mode cluster \
 --conf "spark.pyspark.virtualenv.enabled=true" \
 --conf "spark.pyspark.virtualenv.type=native" \
 --conf "spark.pyspark.virtualenv.bin.path=./venv/bin" \
 --conf "spark.pyspark.python=./venv/bin/python" \
 --jars "spark-avro_2.11-3.2.0.jar" \
 --archives venv.zip#venv \
 --conf "spark.executor.memory=2g" \
 --conf "spark.dynamicAllocation.enabled=True" \
 --conf "spark.dynamicAllocation.minExecutors=12" \
 --conf "spark.driver.memory=20g" \
 --conf "spark.shuffle.service.enabled=True" \
 --conf "spark.yarn.executor.memoryOverhead=10g" \
 --conf "spark.kryoserializer.buffer.max=2047m" \
 --conf "spark.driver.maxResultSize=10g" \
 --conf "spark.rpc.maxSize=1g" \
 --conf "spark.driver.cores=2" \
 --conf "spark.eventLog.enabled=true" \
 --conf "spark.eventLog.dir=hdfs://nameservice1/user/spark/spark2ApplicationHistory"  \
 --conf "spark.yarn.historyServer.address=http://bmw-prod-mn2:18089" \
 --files schema.avsc \
 --py-files "cj_loader.py,cj_predictor.py,cj_export.py" \
 --name "analytic_attributes" \
 --queue root.model.return_model \
 main.py send refit_auto 0.25
