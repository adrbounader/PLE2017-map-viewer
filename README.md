# Map Viewer Application

## To launch the spark app
MASTER=yarn spark-submit \
--jars $(find /home/hadoop/cluster2017/hbase/lib/ -name "*.jar" | tr '\n' ',') \
--num-executors 70 \
--executor-cores 4 \
--executor-memory 512M \
--deploy-mode cluster \
spark-app/target/SparkApp-0.0.1.jar \
<PROGRAM_NAME>

## Contributors
- Adrien Bounader
- Kevin Marzin
