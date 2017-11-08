from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import SQLContext

from pyspark.sql.window import Window
import datetime as dt
import pyspark.sql.functions as func


conf=SparkConf().setMaster("local[4]").setAppName("SparkChallenge.py")
sc=SparkContext(conf=conf)
#Create sqlContext to run queries on dfs
sqlContext=SQLContext(sc)

##Adding header to the data
headersDF = sqlContext.read.format("csv").option("header", "true").load("SPARK_CHALLENGE/datasets/input/site_events_header.csv")
schema = headersDF.schema
df=sqlContext.read.format("csv").option("header","false").option("delimiter","\t").schema(schema).load("SPARK_CHALLENGE/datasets/input/site_events.csv").registerTempTable("site_events")
site_event=sqlContext.sql("select * from site_events")

#Getting only relevant rows associated with tests filtering the ones with test_dtls as NULL
test_users=sqlContext.sql("select * from site_events where test_dtls is not NULL")

##Separate all the tests associated with each user 
user_per_test=test_users.rdd.map(lambda a:[(a[0],a[1],a[2],x.split(":")[0],x.split(":")[1],x.split(":")[2].split("|")[0],x.split(":")[2].split("|")[1]) for x in a[3].split(",")]).flatMap(lambda x:x)

user_per_test_sql=user_per_test.toDF(['v_id','ev_dt','ev_tm','test_id','test_var_id','test_typ_cd','actn_id'])
#Creating a csv dump to verify the results for question1
user_per_test_sql.write.format("csv").option("header","true").option("delimited","true").save("SPARK_CHALLENGE/user_per_test_sql")
#register a temp table to run sql queries
user_per_test_sql.registerTempTable("user_per_test_tbl")

##Question1:
# Use Spark to determine how many unique visitors were included in each test where a test is defined by the test id portion of a test identifier.
visPerTest=sqlContext.sql("select test_id,count(distinct(v_id)) from user_per_test_tbl group by test_id ")
 # visPerTest.take(28)-->Only 27 test_ids will be returned
#Save data into CSV files in the given directory
visPerTest.write.format("csv").option("header","false").option("delimiter",",").save("SPARK_CHALLENGE/datasets/output/answer1_vis_count_per_test")


##Further processing for Question2:
##Converting the timestamp into epoch/posix time to create session_ids
user_per_test_epoch=user_per_test.map(lambda x:(x[0],x[1]
	,x[2],dt.datetime.timestamp(dt.datetime.strptime(x[1]+" "+x[2],"%Y-%m-%d %H:%M:%S")),x[3],x[4],x[5],x[6]))

sDF_user_pt_epoch=user_per_test_epoch.toDF(['v_id','ev_dt','ev_tm','epoch_ts','test_id','test_var_id','test_typ_cd','actn_id'])
sDF_user_pt_epoch.registerTempTable("user_per_test_epoch") 


windowSpec=Window.partitionBy("v_id").orderBy("epoch_ts","v_id")
##Get the difference between timestamps for the current and previous row,and also the track the v_id of prev row
sDF_user_pt_epoch_1=sDF_user_pt_epoch.select("*",func.lag("epoch_ts",1).over(windowSpec).alias("prev_row_epoch"))
sDF_user_pt_epoch_3=sDF_user_pt_epoch_1.select("*",func.lag("v_id").over(windowSpec).alias("prev_v_id")).withColumn("epoch_diff",sDF_user_pt_epoch_1.epoch_ts-sDF_user_pt_epoch_1.prev_row_epoch)


##Add a new column session_id based two conditions:
##1)If the timestamps have a difference greater than or equal to 1800sec(i.e 30mins)
##2)If the current v_id is different than the previous row v_id
sessionWindow=Window.partitionBy("v_id").orderBy("epoch_ts")
##Assigns true(1)/false(0) value if a new session compared to the previous row
IsnewSession=func.when((sDF_user_pt_epoch_3.prev_v_id is sDF_user_pt_epoch_3.v_id ) or (func.abs(sDF_user_pt_epoch_3.epoch_diff)<1800),0).otherwise(1)
newSessionInd=sDF_user_pt_epoch_3.withColumn("IsNewSession",IsnewSession)
sessionWindow=Window.partitionBy("v_id").orderBy("epoch_ts")
session_id=func.sum("IsNewSession").over(sessionWindow)
withSession = newSessionInd.select("v_id","ev_dt","ev_tm","epoch_ts","prev_row_epoch","test_id","IsnewSession").withColumn("session_id", session_id)

###Question2:Use Spark to determine how many visitor sessions were included in each test. A visitor session is defined as a series of clickstream events for a given visitor where there is no gap of 30 minutes of more. If there is a gap of at least 30 minutes, those events should be associated with a new session.
withSession.registerTempTable("withSession_tbl")
visSessions_perTest=sqlContext.sql("select test_id,count(distinct(v_id,session_id)) from withSession_tbl group by test_id order by test_id")

###Question2:Use Spark to determine how many visitor sessions were included in each test. A visitor session is defined as a series of clickstream events for a given visitor where there is no gap of 30 minutes of more. If there is a gap of at least 30 minutes, those events should be associated with a new session.

##View the data in .csv format:

visSessions_perTest.write.format("csv").option("header","false").option("delimiter",",").save("SPARK_CHALLENGE/datasets/output/answer2_vis_session_per_test")



