from pyspark.mllib.feature import HashingTF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.types import *

data = sqlContext.read.json("../data/*.gz")
data.registerTempTable("tuits") 
datanc = sqlContext.sql("select * from tuits where '_corrupt_record' is not null and text is not null")
strpos="select distinct 1,lower(text) from tuits where lower(text) like '%me encanta%' or lower(text) like '%excelente%' or lower(text) like '%que bueno%' or lower(text) like '%maravilla%' or lower(text) like '%que buen%' or lower(text) like '%que bien%' or text like '%:)%' or lower(text) like '%:d' or lower(text) like '%:d %' or text like '%;)%'" 
strneg="select distinct -1,lower(text) from tuits where lower(text) like '%hijue%' or lower(text) like '%malparid%' or lower(text) like '%pésim%' or lower(text) like '%puta%' or lower(text) like '%mierda%'"
queryneg = sqlContext.sql(strneg).rdd
querypos = sqlContext.sql(strpos).rdd
datapos = querypos.map(lambda line : (line[0],line[1]))
dataneg = queryneg.map(lambda line : (line[0],line[1]))
htf=HashingTF(10000)
training = dataneg.union(datapos).map(lambda text : LabeledPoint( text[0], htf.transform(text[1].split(" "))))
lambada=0.2
model = NaiveBayes.train(training, lambada)

# Make prediction and test accuracy.
#predictionAndLabel = training.map(lambda p : (model.predict(p.features), p.label))
#accuracy = 1.0 * predictionAndLabel.filter(lambda x: x[0]== x[1]).count() / training.count()
#model.predict(htf.transform("que dia tan horrible".split(" ")))
#model.predict(htf.transform("hola buenos dias".split(" ")))
#Ahora agregar una columna al dataframe "data" con el resultado del sentiment
sentirdd=datanc.map(lambda x: (x.id,model.predict(htf.transform(x.text.lower().split(" ")))))
fields=[StructField("ids",LongType(),True),StructField("sentiment",StringType(),True)]
sentidf = sqlContext.createDataFrame(sentirdd, StructType(fields))
datafin= datanc.join(sentidf, datanc.id == sentidf.ids))

