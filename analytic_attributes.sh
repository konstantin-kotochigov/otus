export HADOOP_USER_NAME=dmpkit
PYSPARK_PYTHON=./venv/bin/python spark2-submit --master yarn --deploy-mode cluster \
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
 --conf "spark.yarn.executor.memoryOverhead=1024m" \
 --conf "spark.kryoserializer.buffer.max=2047m" \
 --conf "spark.driver.maxResultSize=10g" \
 --conf "spark.rpc.maxSize=1024m" \
 --conf "spark.driver.cores=4" \
 --files schema.avsc \
 --py-files "cj_loader.py,cj_predictor.py,cj_export.py" \
 --name "analytic_attributes" \
 --queue root.model.return_model \
 main.py nosend refit_auto 0.25
