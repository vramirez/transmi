import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
val htf = new HashingTF(10000)
import org.apache.spark.sql.hive.HiveContext
val hctx= new HiveContext(sc)
val queryneg = hctx.sql("select 0,text from tuits.transmi_simple_lower where text like '%:@%'")
val querypos = hctx.sql("select 1,text from tuits.transmi_simple_lower where text like '%me encanta%' or text like '%maravilla%'")
val datapos = querypos.map{line => (line.getInt(0),line.getString(1))}
val dataneg = queryneg.map{line => (line.getInt(0),line.getString(1))}
val training = dataneg.union(datapos).map{ text => new LabeledPoint( text._1, htf.transform(text._2.split(" ")))}
val model = NaiveBayes.train(training)
