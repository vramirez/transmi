import nltk
import re
data = sqlContext.read.json("../data/*.gz")
data.registerTempTable("tuits")                                        
#---- filtrar y extraer tuits no corruptos y no nulos (tuits borrados)
datanc = sqlContext.sql("select lower(text) as tuit from tuits where '_corrupt_record' is not null and text is not null")
texts = datanc.map(lambda x: x.tuit)
words = texts.flatMap(lambda x: re.split("\W+",x))
cwords =words.map(lambda x: (x,1)).reduceByKey( lambda a,b: a+b)
dfwords=cwords.toDF(["guor","caun"])
#dfwords.filter(dfwords["caun"]==1)
#dfwords.filter(dfwords["caun"]>1).orderBy(dfwords.caun.desc()).take(40)
#filtrar palabras menores a 3 caracteres
ndf=dfwords.filter(dfwords["caun"]>1)
ndf.registerTempTable("distrib")
ndf =sqlContext.sql("select guor,caun as cuenta from distrib where length(guor)>2 and caun>1 order by cuenta desc")
#Debo quitar stopwords
stopw=['de', 'en', 'a', 'es', 'los', 'las', 'un', 'unas', 'una', 'por', 'para', 'con', 'el', 'la', 'si', 'no', 'rt', 'del', 'me', 'lo', 'como','pero', 'hay', 'que', 'uno','dos','qué','más','mas','desde','este','éste','esté','esta','está','cuando','ese','esa','eso','todo','les','por','sus','asi','así','nos','solo','porque','son','solo','otros','van','hasta','hace','http','https','fue']
#df1 = sqlContext.createDataFrame(sc.parallelize(stopw).map(lambda x: Row(stpw=x)))
#df1.registerTempTable("stopwt")
##Un dataframe NO es lo mismo que un RDD
ndf.rdd.filter(lambda vec: vec[0] not in stopw).take(30)
#Analisis de HTs
arrh=sqlContext.sql("select entities.hashtags as hashes from tuits where '_corrupt_record' is not null and text is not null and size(entities.hashtags)>0")
hdd=arrh.select("hashes.text").rdd.flatMap(lambda x:x.text)
wc1=hdd.map(lambda x:(x,1)).reduceByKey(lambda a, b: a + b)
wc1.takeOrdered(20, key=lambda x: -x[1])
wc2=wc1.sortBy(lambda x: -x[1])
