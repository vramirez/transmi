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

/* luego de cargar el archivo (imperfecto) no todos los adjetivos tienen feminino y masculino. Acá se corrige eso.*/
val mapaframe=query.map( row => (row.get(0),row.get(1)))
val query=hctx.sql("select * from tuits.adj_f1");
val adj=query.map( row => (row.getString(0)))
val adj_fem= adj.map( line => line.substring(0,line.length-1)+"a")// A todas las palabras les quita la última letra y añade la "a"
val adj_mas= adj.map( line => line.substring(0,line.length-1)+"o")// A todas las palabras les quita la última letra y añade la "o"

/* Se une el distinct masculino con el distinct femenino porque evidentemente quedan repetidos del paso anterior
 * ya que en el archivo original no todos tenían tanto masculino y femenino
 */
adj_mas.distinct.union(adj_fem.distinct).saveAsTextFile("/tmp/adjetivos_all/")
val adj_all=sc.textFile("/tmp/adjetivos_all/part*")
//Lo mismo que la linea anterior pero para dejar en HDFS
adj_mas.distinct.union(adj_fem.distinct).saveAsTextFile("hdfs://localhost:9000/tmp/adjetivos_all/")//
