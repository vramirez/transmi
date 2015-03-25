//Spark
import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
val tmp1=sc.textFile("/tmp/putazos.csv")
//load CSV (un RDD que es un String<array>
//val result=tmp1.map{line => val reader = new CSVReader(new StringReader(line));reader.readNext();}
val raro=tmp1.first.substring(18,19)
val result=tmp1.map{line => line.replaceAll(raro,"")}
val pares=result.map(lines=>(lines.split(",")(0),lines.split(",")(1)))
val parkey=pares.groupByKey()
parkey.first._1
parkey.first._2
