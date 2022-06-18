from pyspark.sql import SparkSession
import os
import subprocess
import sys
import json

# setup arguments
os.environ['kafka1'] = ''
os.environ['kafka2'] = ''
os.environ['schema-registry'] = ''


os.environ['PYSPARK_PYTHON'] = "python3"

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-bundle:1.11.974,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql_2.12:3.0.0,org.apache.kafka:kafka-clients:2.8.0,org.apache.spark:spark-avro_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-streaming_2.12:3.0.0'

def install(package):
    subprocess.check_call(["pip3", "install", package])

install("python-schema-registry-client")
install("schema_registry")

from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
import pyspark.sql.functions as fn
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SparkFiles
import datetime
from pyspark.sql.functions import to_date
import base64
import gzip
from pyspark.sql.functions import max as sparkMax
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, FloatType
from pyspark.sql import DataFrame

spark = SparkSession.builder\
    .appName("pyspark-pipeline")\
    .config("spark.jars", "scalaSpark-assembly-1.0.jar") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql_2.12:3.0.0,org.apache.kafka:kafka-clients:2.8.0,org.apache.spark:spark-avro_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-streaming_2.12:3.0.0')\
    .config("spark.executor.memory", "512m")\
    .config("spark.cores.max", "12")\
    .config("spark.driver.port", "4040")\
    .config("spark.archives","venv1.zip")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()
    
    
dep_file_path = SparkFiles.get("venv1.zip")
spark.sparkContext.addPyFile(dep_file_path)


kafka = spark \
  .read \
  .format("kafka") \
.option("kafka.bootstrap.servers", "")\
.option("subscribePattern", "qatest12nonfes.public.field_data_+(1[0-2]|[1-9])_202[0-9]")\
.option("startingOffsets", "earliest")\
.option("maxOffsetsPerTrigger", 1)\
.option("failOnDataLoss", False)\
.load()

kafka.show(5)


from schema_registry.client import SchemaRegistryClient, schema
from delta.tables import *


def getJsonSchema(schemaId, schemasById, registrySchemaUrl):
        jsonSchema = schemasById.get(schemaId)
        if jsonSchema is None:
            src = SchemaRegistryClient(registrySchemaUrl)
            schema = src.get_by_id(schemaId)
            jsonSchema = json.dumps(schema.flat_schema)
            schemasById[schemaId] = jsonSchema
        return jsonSchema
        
def getJsonSchemaLatestCached(registrySchemaUrl, topicName):
        src = CachedSchemaRegistryClient(registrySchemaUrl)
        schema = src.get_latest_schema(topicName)[1]
        
#         jsonSchema = json.dumps(schema.schema)
        return str(schema)

def getJsonSchemaLatest(registrySchemaUrl, topicName):
        src = SchemaRegistryClient(registrySchemaUrl)
        schema = src.get_schema(subject=topicName).schema
        jsonSchema = json.dumps(schema.flat_schema)
        return jsonSchema
        
kafka = kafka.filter("value is NOT NULL")

sd = datetime.datetime.strptime("03-03-2022", "%d-%m-%Y")

kafka = kafka.filter(kafka['timestamp'] >= sd)
    
byteArrayToLong = fn.udf(lambda x: int.from_bytes(x, byteorder='big', signed=False), LongType())


df = kafka.withColumn('valuenew', kafka['value'])
df = df.withColumn("schemaId", byteArrayToLong(fn.substring("valuenew", 2, 4))) \
                    .withColumn("payload", fn.expr("substring(valuenew, 6, length(valuenew)-5)"))


df.show(10)


dfSchema = getJsonSchemaLatest("", "-value")
df = df.withColumn("payload", fn.expr("substring(valuenew, 6, length(valuenew)-5)"))
df = df.withColumn('value', from_avro("payload", dfSchema))
df = df.drop("payload")

def get_partition_query(partition_column, primary_key):
        if not primary_key or not partition_column:
            return "updatesTable.id = targetTable.id"
        
#         query = "updatesTable.{0} = targetTable.{0}".format(primary_key)
        
        query = ""
        
        primary_keyA = []
    
        pkey_array = primary_key.split(",")
        for partitions in pkey_array:
            partitions = partitions.strip()
            if query == "":
              query += "updatesTable.{0} = targetTable.{0}".format(partitions)
            else:
              query += " and updatesTable.{0} = targetTable.{0}".format(partitions)
              
            primary_keyA.append(partitions)
        
#         primary_key = primary_key.strip()
        partitions_array = partition_column.split(",")
        for partitions in partitions_array:
            partitions = partitions.strip()
            if partitions not in primary_keyA:
                query += " and updatesTable.{0} = targetTable.{0}".format(partitions)
        return query

    
get_partition_query("", "company_id")


  
def getTableColumn(my_schema):
    tableList = []
    col = []
    inputTable = "updatesTable."
    targetTable = "targetTable."

    ignoreColumns = ["table_key_ts", "table_delete", "table_key"]

    for field in my_schema.fields:
        if field.name == "after":
            for f3 in field.dataType:
                te = f3.name + " " + f3.dataType.typeName()
                if f3.name not in ignoreColumns:
                    tableList.append(te)
                    col.append(f3.name)
        else:
            te = field.name + " " + field.dataType.typeName()
            if field.name not in ignoreColumns:
                tableList.append(te)
                col.append(field.name)
    res = ""
    insertResInp = ""
    insertResOut = ""
    merge = ""
    for i in range(len(tableList)):
        if i == (len(tableList) - 1):
            res += tableList[i]
            insertResInp += col[i]
            insertResOut += inputTable + col[i]
            merge += targetTable + col[i] + " = " + inputTable + col[i]
        else:
            res += tableList[i] + ","
            insertResInp += col[i] + ","
            insertResOut += inputTable + col[i] + ","
            merge += targetTable + col[i] + " = " + inputTable + col[i] + ","
    ans = [res, insertResInp, insertResOut, merge]
    
    
    print(ans)
    return ans

def groupk(partition):
      from itertools import groupby
      
      partCount = 1
      l = []
      for key, group in groupby(sorted(partition, key = lambda x: x["table_key"]), lambda x: x["table_key"]):
        a = []
        k = None
        temp = -1

        for thing in group:
          if temp < int(thing["offset"]):
            temp = thing["offset"]
            k = thing

#         yield k
        l.append(k)
        partCount += 1
    
      return l
    
def upsertToDelta2(kafka, batchId):
        byteArrayToLong = fn.udf(lambda x: int.from_bytes(x, byteorder='big', signed=False), LongType())
        df = kafka.withColumn('valuenew', kafka['value'])
        df = df.withColumn("schemaId", byteArrayToLong(fn.substring("valuenew", 2, 4))) \
                            .withColumn("payload", fn.expr("substring(valuenew, 6, length(valuenew)-5)"))


        jsonSchema = getJsonSchema(3737, schemasByIdDict,"http://52.35.125.165:8081")
        currentValueSchemaId = 3737
        currentValueSchema = jsonSchema
        dfBySchemaId = df.where(df.schemaId == currentValueSchemaId)
        dfBySchemaId = dfBySchemaId.drop("value")


        dfBySchemaId = dfBySchemaId.withColumn("value", from_avro("payload", currentValueSchema))
        
        microBatchOutputDF = dfBySchemaId

        df5 = spark.sparkContext._jvm.com.example.Hello.add(spark._jsparkSession, "", "datalake-qa-91")
        microBatchOutputDF = microBatchOutputDF.drop("valuenew")
        
        microBatchOutputDF = microBatchOutputDF.withColumn('valueConsumerKafka', microBatchOutputDF['value'])
        microBatchOutputDF = microBatchOutputDF.drop("value")
        
        microBatchOutputDF.show(1)
        
        
        microBatchOutputDF = microBatchOutputDF.withColumn("table_delete", F.when(
            col("valueConsumerKafka.before").isNotNull and col("valueConsumerKafka.op").isNotNull and col("valueConsumerKafka.op").contains("d"),
            True).otherwise(False))
        
        
        if True:
          microBatchOutputDF = microBatchOutputDF.withColumn("table_key", F.when(col("table_delete") == True,
                                                                                 microBatchOutputDF[
                                                                                     "valueConsumerKafka.before.{}".format(
                                                                                         "id")]).otherwise(
              microBatchOutputDF["valueConsumerKafka.after.{}".format("id")]))

        
          
        
        print("starting point")

        print(datetime.datetime.now())
        
        print("total partitions are")
        print(microBatchOutputDF.rdd.getNumPartitions())
        

        dfrdd = microBatchOutputDF.rdd.mapPartitions(groupk)
        dfByTopic = spark.createDataFrame(dfrdd, schema = microBatchOutputDF.schema)
        
        print(dfByTopic.rdd.getNumPartitions())
        
        print(datetime.datetime.now())
        
#         display(dfByTopic)

        dfByTopicDeleted = dfByTopic.filter(F.col("table_delete") == True)
        dfByTopicDeleted = dfByTopicDeleted.select("valueConsumerKafka.before.*", "table_delete", "table_key", "offset")

        dfByTopicDeleted = dfByTopicDeleted.drop("valueConsumerKafka", "kafka-key", "schemaId")
        dfByTopicDeleted = dfByTopicDeleted.drop("offset")

        dfByTopicInsert = dfByTopic.filter(F.col("table_delete") == False)
        dfByTopicInsert = dfByTopicInsert.select("valueConsumerKafka.after.*", "table_delete", "table_key", "offset")
        # display(df)
        dfByTopicInsert = dfByTopicInsert.drop("valueConsumerKafka", "kafka-key", "schemaId")
        dfByTopicInsert = dfByTopicInsert.drop("offset")
        
        if True:
            partition_column_derive_from_column_split = "created_at_part,created_at".split(",")
            new_partition_key = partition_column_derive_from_column_split[0]
            new_partition_value = partition_column_derive_from_column_split[1]
            
        print(new_partition_value)
        
        if True:
            dfByTopicDeleted = dfByTopicDeleted.withColumn(new_partition_key,to_date(col(new_partition_value)))
            
            dfByTopicInsert = dfByTopicInsert.withColumn(new_partition_key,to_date(col(new_partition_value)))
            
#         if True:
#             my_schema = dfByTopicInsert.schema
#             res = getTableColumn(my_schema)
# #             dfByTopic._jdf.sparkSession().sql("use {}".format(self.database))
#             if True:
#                 print("table created")
#                 dfByTopic._jdf.sparkSession().sql(
#                     "create table if not exists {} ( {} ) using delta PARTITIONED BY ({}) location '{}' ".format("user_summary_old",
#                                                                                                                  res[0],
#                                                                                                                  "company_id,created_at_part",
#                                                                                                                  "user_summary_old"))

#             table_exists = True

        dfByTopicInsert = dfByTopicInsert.drop("table_delete")
        dfByTopicInsert = dfByTopicInsert.drop("table_key")
        
        FullTableName = "user_summary_old"

        print(FullTableName)
        
#         deltaTable=DeltaTable.forName(spark,FullTableName)

        if True:
#             return dfByTopicInsert
#            dfByTopicInsert.write.format("delta").mode("overwrite").save("user_summary_old")
#             query = get_partition_query("company_id,created_at_part", "id")
            
            print("checkpoint 1")
            
#             deltaTable.alias("targetTable").merge(
#                 datau.alias("updatesTable"),
#                 "{0}".format("updatesTable.id = targetTable.id")) \
#               .whenMatchedUpdateAll() \
#               .whenNotMatchedInsertAll().execute()

#             deltaTable.alias("targetTable").merge(
#                 dfByTopicDeleted.alias("updatesTable"),
#                 "{0}".format(query)) \
#               .whenMatchedDelete() \
#               .execute()


#             deltaTable.alias("targetTable").merge(
#                 dfByTopicInsert.alias("updatesTable"),
#                 "{0}".format(query)) \
#               .whenMatchedUpdateAll() \
#               .whenNotMatchedInsertAll().execute()
        
        print("after merge")

        #logger.debug("after merge")
        
        print(datetime.datetime.now())
        
        #logger.debug(str(datetime.datetime.now()))

        
kafka = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "") \
            .option("subscribe", "qatest12nonfes.public.user_summary_old") \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 1000000) \
            .option("failOnDataLoss", False) \
            .load()

schemasByIdDict = {}

kafka = kafka.filter("value is NOT NULL")

ssc = kafka \
            .writeStream \
            .queryName("{0}".format("user_summary_old")) \
            .option("checkpointLocation", "cp/raw/") \
            .trigger(processingTime='10 seconds') \
            .foreachBatch(upsertToDelta2).start()

ssc.awaitTermination()
