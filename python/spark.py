import nltk
import re
data = sqlContext.read.json("/home/vramirez/data/*.gz")
data.registerTempTable("tuits")                                        
#---- filtrar y extraer tuits no corruptos
datanc = sqlContext.sql("select lower(text) as tuit from tuits where '_corrupt_record' is not null and text is not null")
texts = datanc.map(lambda x: x.tuit)
words = texts.flatMap(lambda x: re.split("\s+",x))
cwords =words.map(lambda x: (x,1)).reduceByKey( lambda a,b: a+b)

