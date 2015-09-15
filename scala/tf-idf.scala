//TD-IDF

import org.apache.spark.mllib.clustering.{EMLDAOptimizer, OnlineLDAOptimizer, DistributedLDAModel, LDA}
import org.apache.spark.mllib.feature.{IDF,HashingTF}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.apache.spark.mllib.linalg.{Vector, Vectors}

val tuits= sqlContext.read.json("/data/*.gz")
tuits.registerTempTable("tuits")
val text= sqlContext.sql("select lower(text) from tuits where '_corupt_record' is not null and text is not null")
//val text= sqlContext.sql("select lower(text) from tuits where '_corupt_record' is not null and text is not null and lower(text) not like '%http%'"")
val ltext =text.map(x => x.getString(0) )
//TODO: eliminar las frases que empiecen por http 
//\W también parte las eñes y palabras con tildes =(
val ntext = ltext.flatMap( t => t.split("""\W+"""))
val ntext = ltext.flatMap( t => t.split(" ")).filter( t => ! t.startsWith("http"))
val regex = """[^0-9]*""".r
val sntext= ntext.filter(token=> regex.pattern.matcher(token).matches)
val order = Ordering.by[(String,Int), Int](_._2)
val tokenc = sntext.map(t => (t,1)).reduceByKey(_ + _)
println(ntext.distinct.count)
println( tokenc.top(20)(order).mkString("\n"))
val stopw = Set("de","en","a","es","los","las","un","unas","una","por","para","con","el","la","si","no","rt","del","me","lo","como","pero","hay","que","uno")
val tokensinw = tokenc.filter{case(t,y)=> t.length >2}.filter{case (x,y) => !stopw.contains(x)}
tokensinw.top(20)(order)