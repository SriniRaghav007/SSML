from pyspark import SparkContext 
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import regexp_replace, explode, split


#change payload to a list in stream.py
#change payload[...] = x to payload.append(x)
#working with nested json is a lot harder
def read(rdd):
	if not rdd.isEmpty():
		
		df = spark.read.json(rdd)
		df = df.withColumnRenamed("feature0","Subject").withColumnRenamed("feature1","Message").\
			withColumnRenamed("feature2","Spam")
		subject_col = regexp_replace( regexp_replace(df.Subject, r'(\\n)', ' ') , r'[^a-zA-Z\ ]' , '')
		message_col = regexp_replace( regexp_replace(df.Message, r'(\\n)', ' ') , r'[^a-zA-Z\ ]' , '')

		#Assuming words in both subject as well as message have the same meaning
		#If different, would need to change how union works
		subject_split = df.select(
			explode(split(subject_col, " ")).alias("Words"),
			df.Spam
		)
		message_split = df.select(
			explode(split(message_col, " ")).alias("Words"),
			df.Spam
		)
		words = subject_split.union(message_split)
		#words.show()
		word_counts = words.groupBy("Words","Spam").count()
		word_counts.show()
		#message_split.show()
		#subject_split.show()
	

sc = SparkContext("local[2]", "SpamHamDetection")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)

stream_data = ssc.socketTextStream("localhost", 6100)
stream_data.foreachRDD(lambda rdd: read(rdd))

ssc.start()
ssc.awaitTermination()