--creacion de RDD 
import org.apache.spark.sql.hive.HiveContext
val hctx= new HiveContext(sc)
val query = hctx.sql("select * from tuits.t3 where array_contains(textvec,'hijueputas')")
val mapaframe=query.map( row => (row.get(0),row.get(1)))

select date(ts),count(*) as cuenta from transmi_simple where lower(text) like '%demora%' group by date(ts) order by cuenta;
--Siendo el promedio de 45 tuits al dia puteando, los dias, sobresalen estos 2 días con 
--2015-02-11      343
--2015-02-10      422

--Query de los clientes

select cliente,sum(cuenta) as contador from (select case when lower(source) like '%android%' then 'Android' when source like '%iphone%' or source like '%ipad%' then 'iPhone' when source like '%Windows Phone%' then 'Windows Phon' when source like '%BlackBerry%' then 'BlackBerry' when source like '%Web Client%' then 'Web' when source like '%Facebook%' then 'FBK' when source like '%Mobile Web%' then 'Twitter Mobile' when source like '%transmireporte%' then 'Bot -TransmiReporte' when source like '%TweetDeck%' then 'TweetDeck' else 'Otra vaina' end as cliente,count(*) as cuenta from transmi_tweets group by source) as Q1 group by cliente order by contador;


 select words from t1 join transmi_tweets T on T.id=t1.id where size(words)>1 and text like '%juepu%' limit 10;

--exploracion que trae las distintas palabras despues de una determinada palabra
select distinct textvec[find_in_set("hijueputas",concat_ws(",",textvec))] from t3 where array_contains(textvec,"hijueputas");

--palabra anterior a "servicio"
select palabra,count(*) as cuenta from (select textvec[find_in_set("servicio",concat_ws(",",textvec))-2] as palabra  from t3 where array_contains(textvec,"servicio")) q1 group by palabra order by cuenta;

--la sgte palabra desps d la PALABRA
select palabra,count(*) as cuenta from (select textvec[find_in_set("servicio",concat_ws(",",textvec))] as palabra  from t3 where array_contains(textvec,"hijueputas")) q1 group by palabra order by cuenta; 

--la sgte palabra despues de "servicio tan"
select palabra,count(*) as cuenta from (select textvec[find_in_set("servicio",concat_ws(",",textvec))+1] as palabra  from t3 where array_contains(textvec,"servicio") and textvec[find_in_set("servicio",concat_ws(",",textvec))]="tan") q1 group by palabra order by cuenta;

--busqueda de una palabra y sus 4 variaciones más utilizadas (bloqueo --> bloqueos, hijueputas --> jueputa...)
select word,count(*) as cuenta from t2 where word like '%bloque%' group by word order by cuenta desc limit 5;
select word,count(*) as cuenta from t2 where word like '%gonorr%' group by word order by cuenta desc limit 5;
select word,count(*) as cuenta from t2 where word like '%servic%' group by word order by cuenta desc limit 5;

select palabra,count(*) as cuenta from (select textvec[find_in_set("servicio",concat_ws(",",textvec))-2] as palabra  from t3 where array_contains(textvec,"servicio")) q1 group by palabra order by cuenta;

--pre-identificación de adjetivos
select distinct word from tuits.t2 where (word like '%ado' or word like '%ido' or word like '%ada' or word like '%ida') and (word not like '%\\_%' and word not like '%.%' and word not like '%w%') and length(word)<13 and length(regexp_extract(word,'[a-z]*[0-9]+[a-z]*',0))<=0 order by word
