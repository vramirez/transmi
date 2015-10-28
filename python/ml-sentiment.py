from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import Row

data = sqlContext.read.json("../data/*.gz")
data.registerTempTable("tuits") 
#0 los tuits negativos, 1 los positivos (según la documentacion no acepta negativos)
training = sqlContext.sql("select distinct double(1) as label,lower(text) as text from tuits where lower(text) like '%me encanta%' or lower(text) like '%excelente%' or lower(text) like '%que bueno%' or lower(text) like '%maravilla%' or lower(text) like '%que buen%' or lower(text) like '%que bien%' or text like '%:)%' or lower(text) like '%:d' or lower(text) like '%:d %' or text like '%;)%' union select distinct double(0) as label,lower(text) as text from tuits where lower(text) like '%hijue%' or lower(text) like '%malparid%' or lower(text) like '%pésim%' or lower(text) like '%puta%' or lower(text) like '%mierda%'")
test = sqlContext.sql("select * from tuits where '_corrupt_record' is not null and text is not null")
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
nb = NaiveBayes(smoothing=1.0, modelType="multinomial", labelCol="label")
pipeline = Pipeline(stages=[tokenizer, hashingTF, nb])
model = pipeline.fit(training)
prediction = model.transform(test)
#Según la documentación, salen ordenados, por eso hago filtro para consultar
prediction.select("id", "text", "prediction").filter(prediction["prediction"]==1).take(10)