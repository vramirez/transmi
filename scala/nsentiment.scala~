import org.apache.spark.mllib.clustering.{EMLDAOptimizer, OnlineLDAOptimizer, DistributedLDAModel, LDA}
import org.apache.spark.mllib.feature.{IDF,HashingTF}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.apache.spark.mllib.linalg.{Vector, Vectors}

val tuits= sqlContext.read.json("/data/*.gz")
tuits.registerTempTable("tuits")
val text= sqlContext.sql("select lower(text) from tuits where '_corupt_record' is not null and text is not null")
//cpnvertir 
val ltext =text.map(x => x.getString(0) )
val tx1 =ltext.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))
val tokenized: RDD[Seq[String]] = ltext.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))
//Palabras y sus frecuencias. Las más frecuentes primero. 
val termCounts: Array[(String, Long)] =  tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
val numStopwords = 20
val vocabArray: Array[String] =  termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
//   vocab: Mapa de terminos y frecuencias. -> term index
val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap
// Convert documents into term count vectors
val documents: RDD[(Long, Vector)] =
  tokenized.zipWithIndex.map { case (tokens, id) =>
    val counts = new mutable.HashMap[Int, Double]()
    tokens.foreach { term =>
      if (vocab.contains(term)) {
        val idx = vocab(term)
        counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
      }
    }
    (id, Vectors.sparse(vocab.size, counts.toSeq))
  }

// Set LDA parameters
val numTopics = 10
val lda = new LDA().setK(numTopics).setMaxIterations(10)
val ldaModel = lda.run(documents)
val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
val avgLogLikelihood = distLDAModel.logLikelihood / documents.count()
val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
topicIndices.foreach { case (terms, termWeights) =>
  println("TOPIC:")
  terms.zip(termWeights).foreach { case (term, weight) =>
    println(s"${vocabArray(term.toInt)}\t$weight")
  }
  println()
}
val tf: RDD[Vector] = hashingTF.transform( tokenized)
