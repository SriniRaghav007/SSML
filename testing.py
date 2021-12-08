from pyspark import SparkContext 
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import regexp_replace, explode, split

from sklearn.linear_model import PassiveAggressiveClassifier
from sklearn.feature_extraction.text import HashingVectorizer

import numpy as np
import pickle

def text_preprocess(s):
	o = 1
	if s[2] == "ham":
		o = 0
	return (s[0],s[1],o)

#change payload to a list in stream.py
#change payload[...] = x to payload.append(x)
#working with nested json is a lot harder
	

sc = SparkContext("local[2]", "SpamHamDetection")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)

vectorizer_filename = 'vectorizer.pk'
model_filename = 'model.pk'
vectorizer = pickle.load(open(vectorizer_filename, 'rb'))
model = pickle.load(open(model_filename, 'rb'))

total = 0
correct = 0

def read(rdd):
	if not rdd.isEmpty():

		#global batch
		#batch += 1
		global total 
		global correct

		df = spark.read.json(rdd)
		df = df.withColumnRenamed("feature0","Subject").withColumnRenamed("feature1","Message").\
			withColumnRenamed("feature2","Spam")

		#text_preprocessing 
		df.Subject = regexp_replace( regexp_replace(df.Subject, r'(\\n)', ' ') , r'[^a-zA-Z\ ]' , '')
		df.Message = regexp_replace( regexp_replace(df.Message, r'(\\n)', ' ') , r'[^a-zA-Z\ ]' , '')

		rdd2 = df.rdd.map(text_preprocess)
		df = rdd2.toDF(["Subject","Message","Spam"])

		#messages = np.array(df.select(["Subject","Message"]).rdd.map(lambda r: (r[0],r[1],r[2])).collect())
		messages = np.array(df.collect())
		subject_col = messages[:,0]
		message_col = messages[:,1]
		spam_col = messages[:,2]

		testing_set = vectorizer.fit_transform(message_col)
		#print(training_set)
		testing_res = np.array(spam_col)

		pred = model.predict(testing_set)
		print(("pred",pred,"spam_col",spam_col))
		df.show()

stream_data = ssc.socketTextStream("localhost", 6100)
stream_data.foreachRDD(lambda rdd: read(rdd))

ssc.start()
ssc.awaitTermination()

filename = 'model.pk'
pickle.dump(model, open(filename, 'wb'))
