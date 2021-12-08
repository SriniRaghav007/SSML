from pyspark import SparkContext 
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import regexp_replace, explode, split
from sklearn.cluster import MiniBatchKMeans
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

vectorizer = HashingVectorizer(ngram_range=(1,4), stop_words='english')
model = MiniBatchKMeans(n_clusters=2)
batch = 0

def read(rdd):
	if not rdd.isEmpty():

		global batch
		batch += 1

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

		training_set = vectorizer.fit_transform(message_col)
		#print(training_set)
		training_res = np.array(spam_col)

		model.partial_fit(training_set, training_res)

		#hard-coded batch number
		if batch == 303:
			#print(model.predict(training_set) )
			#print(training_res)
			ssc.stop()

		df.show()

		'''#Assuming words in both subject as well as message have the same meaning
		#If different, would need to change how union works => maybe use weights as an extra column and sum
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
		#subject_split.show()'''

stream_data = ssc.socketTextStream("localhost", 6100)
stream_data.foreachRDD(lambda rdd: read(rdd))

ssc.start()
ssc.awaitTermination()

model_filename = 'model.pk'
pickle.dump(model, open(model_filename, 'wb'))
vectorizer_filename = 'vectorizer.pk'
pickle.dump(vectorizer, open(vectorizer_filename, 'wb'))
