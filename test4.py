from pyspark import SparkContext 
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import regexp_replace
import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer

from sklearn.linear_model import SGDClassifier

import pickle

total = 0
correct = 0



sc = SparkContext("local[2]", "SpamHamDetection")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)



vectorizer = pickle.load(open("savedvector.sav", 'rb'))
model = pickle.load(open("savedfile.sav", 'rb'))



def text_preprocess(s):
	o = 1
	if s[2] == "ham":
		o = 0
	return (s[0],s[1],o)
	
	
count = 0

scores = []
	
	
def read(rdd):
	if not rdd.isEmpty():
		global total
		global correct
		df = spark.read.json(rdd)
		df = df.withColumnRenamed("feature0","Subject").withColumnRenamed("feature1","Message").withColumnRenamed("feature2","Spam")

		#text_preprocessing 
		df.Subject = regexp_replace( regexp_replace(df.Subject, r'(\\n)', ' ') , r'[^a-zA-Z\ ]' , '')
		df.Message = regexp_replace( regexp_replace(df.Message, r'(\\n)', ' ') , r'[^a-zA-Z\ ]' , '')

		rdd2 = df.rdd.map(text_preprocess)
		df = rdd2.toDF(["Subject","Message","Spam"])	
		
		X = vectorizer.fit_transform(np.array(df.select(["Message", "Subject"]).rdd.map(lambda r: r[0]).collect())).toarray()
		y = (np.array(df.select('Spam').rdd.map(lambda r: r[0]).collect()))
		
		pred = model.predict(X)
		total += len(X)
		correct += np.sum(pred == y)
		print((correct/total)*100)


stream_data = ssc.socketTextStream("localhost", 6100)


stream_data.foreachRDD(lambda rdd: read(rdd))


ssc.start()
ssc.awaitTermination()	



























		#print("OK")

