
select date(ts),count(*) as cuenta from transmi_simple where lower(text) like '%juepu%' group by date(ts) order by cuenta;
--Siendo el promedio de 45 tuits al dia puteando, los dias, sobresalen estos 2 días con 
--2015-02-11      343
--2015-02-10      422

--Query para averiguar qué clase de cliente de Twitter es
select cliente,sum(cuenta) as contador from (select case when lower(source) like '%android%' then 'Android' when source like '%iphone%' or source like '%ipad%' then 'iPhone' when source like '%Windows Phone%' then 'Windows Phon' when source like '%BlackBerry%' then 'BlackBerry' when source like '%Web Client%' then 'Web' when source like '%Facebook%' then 'FBK' when source like '%Mobile Web%' then 'Twitter Mobile' when source like '%transmireporte%' then 'Bot -TransmiReporte' when source like '%TweetDeck%' then 'TweetDeck' else 'Otra vaina' end as cliente,count(*) as cuenta from transmi_tweets group by source) as Q1 group by cliente order by contador;

--exploracion que trae las distintas palabras despues de una determinada palabra
select palabra,count(*) as cuenta from (select textvec[find_in_set("policia",concat_ws(",",textvec))] as palabra  from t3 where array_contains(textvec,"policia")) q1 group by palabra order by cuenta;

--palabra anterior a "servicio"
select palabra,count(*) as cuenta from (select textvec[find_in_set("servicio",concat_ws(",",textvec))-2] as palabra  from t3 where array_contains(textvec,"servicio")) q1 group by palabra order by cuenta;

--la sgte palabra despues de "servicio tan"
select palabra,count(*) as cuenta from (select textvec[find_in_set("servicio",concat_ws(",",textvec))+1] as palabra  from t3 where array_contains(textvec,"servicio") and textvec[find_in_set("servicio",concat_ws(",",textvec))]="tan") q1 group by palabra order by cuenta;

--busqueda de una palabra y sus 4 variaciones más utilizadas (bloqueo --> bloqueos, hijueputas --> jueputa...)
select word,count(*) as cuenta from t2 where word like '%bloque%' group by word order by cuenta desc limit 5;
select word,count(*) as cuenta from t2 where word like '%gonorr%' group by word order by cuenta desc limit 5;
select word,count(*) as cuenta from t2 where word like '%servic%' group by word order by cuenta desc limit 5;

--tweets con rabia o negativos
select distinct text from tuits.transmi_simple_lower where text like '%:@%' or text like '%me emputa%' or text like '%mal servicio%';

--tweets de positivos
select distinct text from tuits.transmi_simple_lower where text like '%me encanta%' or text like '%maravilla%' or text like '%:)%' or text like '%:d' or text like '%:d %' ;

--EXPLORACIÓN
--Después de la palabra "transmilenio", quito las palabras como "por,para,de,..." con length(palabra)>4
select palabra,count(*) as cuenta from (select textvec[find_in_set("transmilenio",concat_ws(",",textvec))] as palabra  from t3 where array_contains(textvec,"transmilenio")) q1 where length(palabra)>4 group by palabra order by cuenta;

select palabra,count(*) as cuenta from (select textvec[find_in_set("buen",concat_ws(",",textvec))] as palabra  from t3 where array_contains(textvec,"buen")) q1 group by palabra order by cuenta;

select palabra,count(*) as cuenta from (select textvec[find_in_set("mal",concat_ws(",",textvec))] as palabra  from t3 where array_contains(textvec,"mal")) q1 group by palabra order by cuenta;

--cruce de tuist negativos por hora
select hour(ts),count(feeling) value from transmi_simple T join sentims S on s.id =t.id where feeling=-1 group by hour(ts) order by value;

--cruce de tuist negativos por hora
