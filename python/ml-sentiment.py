from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import HashingTF, Tokenizer,RegexTokenizer
from pyspark.sql import Row
from pyspark.sql.functions import hour,date_format

data = sqlContext.read.json("../data/*.gz")
data.registerTempTable("tuits") 
#0 los tuits negativos, 1 los positivos (según la documentacion no acepta negativos)
training = sqlContext.sql("select distinct double(1) as label,lower(text) as minus from tuits where lower(text) like '%me encanta%' or lower(text) like '%excelente%' or lower(text) like '%que bueno%' or lower(text) like '%maravilla%' or lower(text) like '%que buen%' or lower(text) like '%que bien%' or text like '%:)%' or lower(text) like '%:d' or lower(text) like '%:d %' or text like '%;)%' union select distinct double(0) as label,lower(text) as minus from tuits where lower(text) like '%hijue%' or lower(text) like '%malparid%' or lower(text) like '%pésim%' or lower(text) like '%puta%' or lower(text) like '%mierda%'")
test = sqlContext.sql("select *,lower(text) minus from tuits where '_corrupt_record' is not null and text is not null")
#tokenizer = Tokenizer(inputCol="minus", outputCol="words")
tokenizer = RegexTokenizer(inputCol="minus", outputCol="words", pattern="\\W+",minTokenLength=3)
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
nb = NaiveBayes(smoothing=1.0, modelType="multinomial", labelCol="label")
pipeline = Pipeline(stages=[tokenizer, hashingTF, nb])
model = pipeline.fit(training)
prediction = model.transform(test)
#Según la documentación, salen ordenados, por eso hago filtro para consultar
prediction.filter(prediction.sentiment=1).select("id", "text", "words").take(10)
prediction.registerTempTable("predicts")
sqlContext.sql("select id,prediction,cast ( from_unixtime( unix_timestamp(concat( substring(created_at,27,4),' ', substring(created_at,5,15)), 'yyyy MMM dd hh:mm:ss')-18000) as timestamp) ts from predicts")
predict.select("prediction",hour("ts")).groupBy("hour(ts)").count()
sqlContext.sql("select hour(ts) hora,prediction ,count(*) ntuits from predict2 group by prediction,hour(ts) order by hora").show(100)
predict.select(hour("ts").alias("hora"),"prediction").groupBy("hora","prediction").count().orderBy("hora").collect()