from pyspark import SparkContext 
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import regexp_replace
import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer

from sklearn.linear_model import SGDClassifier

import pickle




sc = SparkContext("local[2]", "SpamHamDetection")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)

hashing = HashingVectorizer(stop_words = 'english')





def text_preprocess(s):
	o = 1
	if s[2] == "ham":
		o = 0
	return (s[0],s[1],o)
	
	
count = 0

scores = []
	
	
def read(rdd):
	if not rdd.isEmpty():
		
		df = spark.read.json(rdd)
		df = df.withColumnRenamed("feature0","Subject").withColumnRenamed("feature1","Message").withColumnRenamed("feature2","Spam")

		#text_preprocessing 
		df.Subject = regexp_replace( regexp_replace(df.Subject, r'(\\n)', ' ') , r'[^a-zA-Z\ ]' , '')
		df.Message = regexp_replace( regexp_replace(df.Message, r'(\\n)', ' ') , r'[^a-zA-Z\ ]' , '')

		rdd2 = df.rdd.map(text_preprocess)
		df = rdd2.toDF(["Subject","Message","Spam"])	
		
		#messages = np.array(df.select(["Subject","Message", "Spam"]).rdd.map(lambda r: (r[0],r[1], r[2])).collect())
		#subject_col = messages[:,0]
		#message_col = messages[:,1]
		#spam_col = messages[:, 2]
		
		global count
		model = train(df)
		count = count + 1
		pickle.dump(model, open("savedfile.sav", 'wb'))
		pickle.dump(hashing, open("savedvector.sav", 'wb'))
		print(count)
		if(count == 303):
			ssc.stop()
			#return count

		#print("OK")



def train(df):
	
	X = hashing.fit_transform(np.array(df.select(["Message", "Subject"]).rdd.map(lambda r: r[0]).collect())).toarray()
	y = (np.array(df.select('Spam').rdd.map(lambda r: r[0]).collect()))
	#X.reshape(-1, 1)
	clf = SGDClassifier(penalty='l2',loss='hinge')
	prd = clf.partial_fit(X, y, classes=np.unique(y))

	return prd	


def evaluate():
	model = pickle.load(open("savedfile.sav", 'rb'))
	pred = model.predict(df)
	total += len(messages)
	correct += np.sum(pred == testing_res)




stream_data = ssc.socketTextStream("localhost", 6100)


stream_data.foreachRDD(lambda rdd: read(rdd))


ssc.start()
ssc.awaitTermination()	
		
	



