--tabla external transmi_tweets que apunta a todos los tweets disponibles en /data (que a su vez contiene los .json comprimidos o no)

CREATE  TABLE transmi_tweets (
   id BIGINT,
   created_at STRING,
   source STRING,
   lang STRING,
   favorited BOOLEAN,
   retweet_count INT,
   retweeted_status STRUCT<
      text:STRING,
      user:STRUCT<screen_name:STRING,name:STRING>>,
   entities STRUCT<
      urls:ARRAY<STRUCT<expanded_url:STRING>>,
      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
      hashtags:ARRAY<STRUCT<text:STRING>>>,
   text STRING,
   user STRUCT<
      screen_name:STRING,
      name:STRING,
      friends_count:INT,
      followers_count:INT,
      statuses_count:INT,
      verified:BOOLEAN,
      utc_offset:STRING, -- was INT but nulls are strings
      time_zone:STRING>,
   in_reply_to_screen_name STRING,
   year int,
   month int,
   day int,
   hour int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/data';


-- Clean up tweets
CREATE OR REPLACE VIEW transmi_simple AS
SELECT
  id,
  lower(user.screen_name) as username,
  cast ( from_unixtime( unix_timestamp(concat( substring(created_at,27,4),' ', substring(created_at,5,15)), 'yyyy MMM dd hh:mm:ss')-18000) as timestamp) ts,
  text  
FROM transmi_tweets;

-- 
eCREATE OR REPLACE VIEW transmi_simple_lower AS
SELECT
  id,
  lower(user.screen_name) as username,
  cast ( from_unixtime( unix_timestamp(concat( substring(created_at,27,4),' ', substring(created_at,5,15)), 'yyyy MMM dd hh:mm:ss')-18000) as timestamp) ts,
  lower(text)  text
FROM transmi_tweets;

-- Compute sentiment
create or replace view t1 as select id, words from transmi_tweets lateral view explode(sentences(lower(text))) dummy as words;
create or replace view t2 as select id, word from t1 lateral view explode( words ) dummy as word ;
create table t3 as select id,collect_list(word) textvec from t2 group by id;


create table adj_f1(adjetivos varchar(30));
LOAD DATA LOCAL INPATH '/tmp/adjetivos.txt' into table adj_f1;

