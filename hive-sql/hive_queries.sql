
select date(ts),count(*) as cuenta from transmi_simple where lower(text) like '%juepu%' group by date(ts) order by cuenta;
--Siendo el promedio de 45 tuits al dia puteando, los dias, sobresalen estos 2 días con 
--2015-02-11      343
--2015-02-10      422


select ngrams(sentences(lower(text)),1,10) from t_borrame1; --No dice nada interesante

--create table t_borrame1 as select lower(text) text,ts from transmi_simple where date(ts)=date('2015-02-11');
create table t_feb10 as select * from transmi_simple where ts>=date('2015-02-10') and ts <date('2015-02-11');
--create table t_borrame2 as select lower(text) text,ts from transmi_simple where date(ts)=date('2015-02-10');
create table t_feb11 as select * from transmi_simple where ts>=date('2015-02-11') and ts <date('2015-02-12');

--El dia 10 y 11 es un solo tuit retuiteado cientos de veces a las 9pm

--Ya bajé el src de Hive y Spark nuevos

--filtrar las puteadas pro proximidad a la palabra TRANSMILENIO(?????)

--Query de los clientes

select cliente,sum(cuenta) as contador from (select case when lower(source) like '%android%' then 'Android' when source like '%iphone%' or source like '%ipad%' then 'iPhone' when source like '%Windows Phone%' then 'Windows Phon' when source like '%BlackBerry%' then 'BlackBerry' when source like '%Web Client%' then 'Web' when source like '%Facebook%' then 'FBK' when source like '%Mobile Web%' then 'Twitter Mobile' when source like '%transmireporte%' then 'Bot -TransmiReporte' when source like '%TweetDeck%' then 'TweetDeck' else 'Otra vaina' end as cliente,count(*) as cuenta from transmi_tweets group by source) as Q1 group by cliente order by contador;

/home/vramirez/Documents/Lin_Kolcz_SIGMOD2012.pdf
/home/vramirez/Documents/PracticalMachineLearning.pdf

 select words from t1 join transmi_tweets T on T.id=t1.id where size(words)>1 and text like '%juepu%' limit 10;
