from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import HashingTF, Tokenizer,RegexTokenizer
from pyspark.sql import Row
from pyspark.sql.functions import hour,date_format

from pyspark import SparkContext
from pyspark.sql import HiveContext


sc = SparkContext("local[*]", "Sentiment Analysis ML")
sqlContext = HiveContext(sc)


data = sqlContext.read.json("../data/*.gz")
data.registerTempTable("tuits") 
#0 los tuits negativos, 1 los positivos (según la documentacion no acepta negativos)
training = sqlContext.sql("select distinct double(1) as label,lower(text) as minus from tuits where lower(text) like '%me encanta%' or lower(text) like '%excelente%' or lower(text) like '%maravill%' or lower(text) like '% buen%' or lower(text) like '%que bien%' or text like '%:)%' or lower(text) like '%:d' or lower(text) like '%:d %' or text like '%;)%' union select distinct double(0) as label,lower(text) as minus from tuits where lower(text) like '%hijue%' or lower(text) like '%malparid%' or lower(text) like '%pésim%' or lower(text) like '%puta%' or lower(text) like '%mierda%' or lower(text) like '%pesim%' or lower(text) like '%verga%'")
training.groupby("label").count().show()
test = sqlContext.sql("select *,lower(text) minus from tuits where '_corrupt_record' is not null and text is not null")
#tokenizer = Tokenizer(inputCol="minus", outputCol="words")
tokenizer = RegexTokenizer(inputCol="minus", outputCol="words", pattern="\\W+",minTokenLength=3)
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
nb = NaiveBayes(smoothing=1.0, modelType="multinomial", labelCol="label")
pipeline = Pipeline(stages=[tokenizer, hashingTF, nb])
model = pipeline.fit(training)
prediction = model.transform(test)
#Según la documentación, salen ordenados, por eso hago filtro para consultar
prediction.registerTempTable("predicts")
predict=sqlContext.sql("select id,prediction,cast ( from_unixtime( unix_timestamp(concat( substring(created_at,27,4),' ', substring(created_at,5,15)), 'yyyy MMM dd HH:mm:ss')-18000) as timestamp) ts from predicts")
predict.registerTempTable("predict2")
#predict.select("prediction",hour("ts")).groupBy("hour(ts)").count()
sqlContext.sql("select hour(ts) hora,prediction ,count(*) ntuits from predict2 group by hour(ts),prediction order by hora").show(100)
#predict.select(hour("ts").alias("hora"),"prediction").groupBy("hora","prediction").count().orderBy("hora").show()
