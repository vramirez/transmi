import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
val htf = new HashingTF(10000)
import org.apache.spark.sql.hive.HiveContext
val hctx= new HiveContext(sc)
val strpos= "select distinct 1,text from tuits.transmi_simple_lower where text like '%me encanta%' or text like '%maravilla%' or text like '%:)%' or text like '%:d' or text like '%:d %' "
val strneg = "select distinct -1,text from tuits.transmi_simple_lower where text like '%:@%' or text like '%me emputa%' or text like '%mal servicio%' union select distinct -1,lower(text)  from t3 join transmi_simple_lower t on t3.id=t.id where array_contains(textvec,"hijueputa") and textvec[find_in_set('hijueputa',concat_ws(",",textvec))]='transmilenio' "
val queryneg = hctx.sql(strneg)
val querypos = hctx.sql(strpos)
val alldata = hctx.sql("select id,text from tuits.transmi_simple_lower where id is not null")
val datapos = querypos.map{line => (line.getInt(0),line.getString(1))}
val dataneg = queryneg.map{line => (line.getInt(0),line.getString(1))}
val training = dataneg.union(datapos).map{ text => new LabeledPoint( text._1, htf.transform(text._2.split(" ")))}
//val model = NaiveBayes.train(training)
//model.predict( htf.transform("que mierda montar este transmilenio"))
//Calcular efectividad comparando predicciones con la data de training
val lambada= 0.2
val model = NaiveBayes.train(training,lambda=lambada)
val predictionAndLabel = training.map(p => (model.predict(p.features), p.label))
val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / training.count()
val datardd = alldata.map( line=> (line.getLong(0),line.getString(1)))
val sentimap = datardd.map( line=> (line._1,model.predict(htf.transform(line._2.split(" ")))))
val tab1=hctx.createDataFrame(sentimap)
tab1.registerTempTable("sentitab")
val results = hctx.sql("select count(*) from sentitab")
//confrontar el modelo con la data
val queryall = hctx.sql("select text from tuits.transmi_simple_lower")
val opinions= queryall.map(line => line.getString(0))
val sentims = opinions.map( line => ( model.predict( htf.transform(line.split(" "))),line))
sentims.saveAsTextFile("/tmp/sentimientos.out/")

--ROW FORMAT DELIMITED FIELDS TERMINATED BY 
val clean_sentims= sc.textFile("/tmp/sentims/")
val clean2=clean_sentims.map(line=> line.replace("(",""))
val clean3=clean2.map(line=> line.replace(")",""))
clean3.saveAsTextFile("/tmp/sentims2")




