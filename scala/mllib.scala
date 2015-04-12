import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
val htf = new HashingTF(10000)
val positiveData = sc.textFile("/root/spark-1.3.0/txt_sentoken/positive.txt")
val negativeData = sc.textFile("/root/spark-1.3.0/txt_sentoken/negative.txt")
val menos = negativeData.map{text => new LabeledPoint(0, htf.transform(text.split(" ")))}
val mas = positiveData.map{text => new LabeledPoint(1, htf.transform(text.split(" ")))}
val posSplits= mas.randomSplit(Array(0.4,0.6),seed=11L)
val negSplits= menos.randomSplit(Array(0.4,0.6),seed=11L)
val training = posSplits(0).union(negSplits(0))
val test = posSplits(1).union(negSplits(1))
val model = NaiveBayes.train(training)
val predictionAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
//val predictionAndLabels = test.map { point => (model.predict(point.features),point.label)}
val metrics = new MulticlassMetrics(predictionAndLabels)
metrics.labels.foreach( l => println(metrics.fMeasure(l)))
val accuracy = 1.0 * predictionAndLabels.filter(x => x._1 == x._2).count() / test.count()

