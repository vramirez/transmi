import scala.collection.mutable
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

// Load documents from text files, 1 document per file
val corpus: RDD[String] = sc.wholeTextFiles("../*tuit*").map(_._2)

val stopw = Set("ante","de","en","a","es","los","las","un","unas","una","por","para","con","el","la","si","no","rt","del","me","lo","como","pero","hay","que","uno","esta","este","hasta","cuando","porque","ahora","sin","mas","más","hoy","son","qué","así")

// Split each document into a sequence of terms (words)
val tokenized: RDD[Seq[String]] =
  corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))

// Choose the vocabulary.
//   termCounts: Sorted list of (term, termCount) pairs
val termCountz: Array[(String, Long)] =  tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
//   vocabArray: Chosen vocab (removing common terms)
val termCounts = termCountz.filter{case(t,y)=> t.length >2}.filter{case (x,y) => !stopw.contains(x)}
val numStopwords = 20
val vocabArray: Array[String] =
  termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
//   vocab: Map term -> term index
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
val numTopics = 5
val lda = new LDA().setK(numTopics).setMaxIterations(10)

val ldaModel = lda.run(documents)
val avgLogLikelihood = ldaModel.logLikelihood / documents.count()

// Print topics, showing top-weighted 10 terms for each topic.
val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
topicIndices.foreach { case (terms, termWeights) =>
  println("TOPIC:")
  terms.zip(termWeights).foreach { case (term, weight) =>
    println(s"${vocabArray(term.toInt)}\t$weight")
  }
  println()
}
