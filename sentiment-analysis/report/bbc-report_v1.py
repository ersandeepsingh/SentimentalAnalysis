from neo4j import GraphDatabase
from googletrans import Translator
translator = Translator()
from dateutil.parser import parse
from sys import argv
import random

#driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "root"))
driver = GraphDatabase.driver("bolt://ec2-3-94-239-53.compute-1.amazonaws.com:7687", auth=("neo4j", "i-0d41170bde89dba5a"))
# print(event)
update_count_query = """
Match 
    (c:Country {name: $country})<-[r1:SOURCE_COUNTRY]-(s:Source {name: $source})<-[r2:TOPIC_SOURCE]-(t:Topic{name: $topic})<-[rd:DATE_TOPIC]-(d:Date {date: $date}),
    (tt:Tweet {tweet_id: $tweet_id}) 
Set 
    d.totalMentions = d.totalMentions + 1,
    t.totalMentions = t.totalMentions + 1, 
    s.totalMentions = s.totalMentions + 1, 
    c.totalMentions = c.totalMentions + 1, 
    d.totalSentiment = d.totalSentiment + tt.score,
    t.totalSentiment = t.totalSentiment + tt.score,
    s.totalSentiment = s.totalSentiment + tt.score,
    c.totalSentiment = c.totalSentiment + tt.score,
    d.positiveSentiment = Case When tt.score >0 Then d.positiveSentiment+tt.score else d.positiveSentiment End ,
    t.positiveSentiment = Case When tt.score >0 Then t.positiveSentiment+tt.score else t.positiveSentiment End ,
    s.positiveSentiment = Case When tt.score >0 Then s.positiveSentiment+tt.score else s.positiveSentiment End ,
    c.positiveSentiment = Case When tt.score >0 Then c.positiveSentiment+tt.score else c.positiveSentiment End ,

    d.positiveMentions = Case When tt.score >0 Then d.positiveMentions + 1 else d.positiveMentions  End ,
    t.positiveMentions = Case When tt.score >0 Then t.positiveMentions + 1 else t.positiveMentions  End ,
    s.positiveMentions = Case When tt.score >0 Then s.positiveMentions + 1 else s.positiveMentions  End ,
    c.positiveMentions = Case When tt.score >0 Then c.positiveMentions + 1 else c.positiveMentions  End ,

    d.negativeSentiment = Case When tt.score <0 Then d.negativeSentiment + tt.score else d.negativeSentiment  End ,
    t.negativeSentiment = Case When tt.score <0 Then t.negativeSentiment + tt.score else t.negativeSentiment  End ,
    s.negativeSentiment = Case When tt.score <0 Then s.negativeSentiment + tt.score else s.negativeSentiment  End ,
    c.negativeSentiment = Case When tt.score <0 Then c.negativeSentiment + tt.score else c.negativeSentiment  End ,

    d.negativeMentions = Case When tt.score <0 Then d.negativeMentions +1 else d.negativeMentions  End ,
    t.negativeMentions = Case When tt.score <0 Then t.negativeMentions +1 else t.negativeMentions  End ,
    s.negativeMentions = Case When tt.score <0 Then s.negativeMentions +1 else s.negativeMentions  End ,
    c.negativeMentions = Case When tt.score <0 Then c.negativeMentions +1 else c.negativeMentions  End ,

    d.neutralMentions = Case When tt.score =0 Then d.neutralMentions+1 else d.neutralMentions End ,
    t.neutralMentions = Case When tt.score =0 Then t.neutralMentions+1 else t.neutralMentions End ,
    s.neutralMentions = Case When tt.score =0 Then s.neutralMentions+1 else s.neutralMentions End ,
    c.neutralMentions = Case When tt.score =0 Then c.neutralMentions+1 else c.neutralMentions End ,

    d.weakPositiveMentions = Case When tt.score >0 Then Case when tt.score <=0.30 Then d.weakPositiveMentions+1 else d.weakPositiveMentions End else d.weakPositiveMentions End ,
    t.weakPositiveMentions = Case When tt.score >0 Then Case when tt.score <=0.30 Then t.weakPositiveMentions+1 else t.weakPositiveMentions End else t.weakPositiveMentions End ,
    s.weakPositiveMentions = Case When tt.score >0 Then Case when tt.score <=0.30 Then s.weakPositiveMentions+1 else s.weakPositiveMentions End else s.weakPositiveMentions End ,
    c.weakPositiveMentions = Case When tt.score >0 Then Case when tt.score <=0.30 Then c.weakPositiveMentions+1 else c.weakPositiveMentions End else c.weakPositiveMentions End ,

    d.mildPositiveMentions = Case When tt.score >0.3 Then Case when tt.score <=0.70 Then d.mildPositiveMentions+1 else d.mildPositiveMentions End else d.mildPositiveMentions End ,
    t.mildPositiveMentions = Case When tt.score >0.3 Then Case when tt.score <=0.70 Then t.mildPositiveMentions+1 else t.mildPositiveMentions End else t.mildPositiveMentions End ,
    s.mildPositiveMentions = Case When tt.score >0.3 Then Case when tt.score <=0.70 Then s.mildPositiveMentions+1 else s.mildPositiveMentions End else s.mildPositiveMentions End ,
    c.mildPositiveMentions = Case When tt.score >0.3 Then Case when tt.score <=0.70 Then c.mildPositiveMentions+1 else c.mildPositiveMentions End else c.mildPositiveMentions End ,

    d.strongPositiveMentions = Case When tt.score >0.7 Then d.strongPositiveMentions+1 else d.strongPositiveMentions End ,
    t.strongPositiveMentions = Case When tt.score >0.7 Then t.strongPositiveMentions+1 else t.strongPositiveMentions End ,
    s.strongPositiveMentions = Case When tt.score >0.7 Then s.strongPositiveMentions+1 else s.strongPositiveMentions End ,
    c.strongPositiveMentions = Case When tt.score >0.7 Then c.strongPositiveMentions+1 else c.strongPositiveMentions End ,

    d.weakNegativeMentions = Case When tt.score <0 Then Case when tt.score >=-0.30 Then d.weakNegativeMentions+1 else d.weakNegativeMentions End else d.weakNegativeMentions End ,
    t.weakNegativeMentions = Case When tt.score <0 Then Case when tt.score >=-0.30 Then t.weakNegativeMentions+1 else t.weakNegativeMentions End else t.weakNegativeMentions End ,
    s.weakNegativeMentions = Case When tt.score <0 Then Case when tt.score >=-0.30 Then s.weakNegativeMentions+1 else s.weakNegativeMentions End else s.weakNegativeMentions End ,
    c.weakNegativeMentions = Case When tt.score <0 Then Case when tt.score >=-0.30 Then c.weakNegativeMentions+1 else c.weakNegativeMentions End else c.weakNegativeMentions End ,

    d.mildNegativeMentions = Case When tt.score <-0.3 Then Case when tt.score >=-0.70 Then d.mildNegativeMentions+1 else d.mildNegativeMentions End else d.mildNegativeMentions End ,
    t.mildNegativeMentions = Case When tt.score <-0.3 Then Case when tt.score >=-0.70 Then t.mildNegativeMentions+1 else t.mildNegativeMentions End else t.mildNegativeMentions End ,
    s.mildNegativeMentions = Case When tt.score <-0.3 Then Case when tt.score >=-0.70 Then s.mildNegativeMentions+1 else s.mildNegativeMentions End else s.mildNegativeMentions End ,
    c.mildNegativeMentions = Case When tt.score <-0.3 Then Case when tt.score >=-0.70 Then c.mildNegativeMentions+1 else c.mildNegativeMentions End else c.mildNegativeMentions End ,

    d.strongNegativeMentions = Case When tt.score <-0.7 Then d.strongNegativeMentions+1 else d.strongNegativeMentions End ,
    t.strongNegativeMentions = Case When tt.score <-0.7 Then t.strongNegativeMentions+1 else t.strongNegativeMentions End ,
    s.strongNegativeMentions = Case When tt.score <-0.7 Then s.strongNegativeMentions+1 else s.strongNegativeMentions End ,
    c.strongNegativeMentions = Case When tt.score <-0.7 Then c.strongNegativeMentions+1 else c.strongNegativeMentions End 
"""

# topic = "saudiarabia"
# source = "bbc"


def lambda_handler(source, topic, filename):
    import json
    import config
    # body = json.loads(event.get('Records')[0]['body'])
    # filename = body['file']

    context_data = read_file_form_s3(config.S3_BUCKET, filename)
    try:
        result = [json.loads(item) for item in context_data]
        print(len(result))
        for index, data in enumerate(result):

            with driver.session() as session:

                date = parse(data['publishedAt'])
                date_string = "{:%Y-%m-%d}".format(date)
                date_type = 'daily'

                # code for date aggregation
                run_query(session, add_date_topic_source,
                          date_string, date_type, topic, source)
                sentiment = data['analysis']['sentiment']['documentSentiment']
                score = sentiment.get('score') if sentiment.get('score') else 0
                tweet_id = random.randint(0,1000000000000)
                data['user'] = {'id': random.randint(0,1000000000000),'profile_image_url': data['urlToImage'], 'name': data['title'], 'screen_name': data['url']}

                # print(data.get('text'))
                # print(data.get('user'))
                if not session.read_transaction(isTweetExist, tweet_id):
                    print("data.get('lang')", data.get('lang'))
                    language = data.get('lang') if data.get('lang') else 'en'
                    run_query(session, add_tweet, tweet_id, score, data.get(
                        'publishedAt'), data.get('content'), data.get('user'),language , "bbc")
                    print("tweet added")
                    try:
                        loc = None#translator.translate(data['user']['location']).text
                        loc = loc if loc else 'uk'
                        run_query(session, add_country,
                                date_string, topic, source, loc)
                        run_query(session, update_all_count,
                                date_string, topic, source, loc, tweet_id)
                        run_query(session, add_country_tweet_relation,
                                date_string, topic, source, loc, tweet_id)
                        for entitites in data['analysis']['classifiction']['categories']:
                            try:
                                entity_list = []
                                entitites = entitites.get('name').split('/')
                                for e in entitites:
                                    if e in ['', None, "", " "]:
                                        continue
                                    d = e.split('&')
                                    entity_list.extend(d)
                                for entity in set(entity_list):
                                    entity = entity.strip()
                                    if entity in ['', None, "", " "]:
                                        continue
                                    run_query(session, add_entity, date_string,
                                            topic, source, loc, entity)
                                    run_query(session, add_entity_tweet_relation, date_string,
                                            topic, source, loc, entity, tweet_id)
                            except Exception as e:
                                print("entity:", e)
                    except Exception as e:
                        print("upper", e)

                    # #code for month aggregation
                    month_string = "{:%Y-%m}".format(date)
                    date_type = 'monthly'
                    run_query(session, add_date_topic_source,
                            month_string, date_type, topic, source)
                    sentiment = data['analysis']['sentiment']['documentSentiment']
                    score = sentiment.get('score') if sentiment.get('score') else 0

                    # print(data['user'])
                    # run_query(session, add_tweet, tweet_id, score)
                    try:
                        print("loc")
                        loc = None#translator.translate(data['user']['location']).text
                        loc = loc if loc else 'uk'
                        run_query(session, add_country,
                                month_string, topic, source, loc)
                        run_query(session, update_all_count,
                                month_string, topic, source, loc, tweet_id)
                        run_query(session, add_country_tweet_relation,
                                month_string, topic, source, loc, tweet_id)
                        for entitites in data['analysis']['classifiction']['categories']:
                            try:
                                entity_list = []
                                entitites = entitites.get('name').split('/')
                                for e in entitites:
                                    if e in ['', None, "", " "]:
                                        continue
                                    d = e.split('&')
                                    entity_list.extend(d)
                                for entity in entity_list:
                                    entity = entity.strip()
                                    print("--{}--".format(entity))
                                    if entity in ['', None, "", " "]:
                                        continue
                                    run_query(session, add_entity, month_string,
                                            topic, source, loc, entity)
                                    run_query(session, add_entity_tweet_relation, month_string,
                                            topic, source, loc, entity, tweet_id)
                            except Exception as e:
                                print("entity:", e)
                    except Exception as e:
                        print(e)
                    print("completed---------{}".format(index))
                    session.success = True
                    # break
                else:
                    print("tweet already exist: {}".format(tweet_id))
    except Exception as e:
        print(e)
    return result


def run_query(session, *args):
    session.write_transaction(*args)

def isTweetExist(tx, tweet_id):
    id =None
    for tweet in tx.run("Match(tt:Tweet {tweet_id:$tweet_id}) Return tt.tweet_id", tweet_id=tweet_id):
        print(tweet['tt.tweet_id'])
        id = tweet['tt.tweet_id']
        break
    print(id)
    return id

def update_all_count(tx, date, topicName, sourceName, countryName, tweet_id):
    tx.run(update_count_query, date=date, topic=topicName,
           source=sourceName, country=countryName, tweet_id=tweet_id)


def add_tweet(tx, tweet_id, score, timestamp, text, user_data, lang, source):
    print("adding tweet {} {} {} {} ".format(tweet_id, score,
                                             user_data.get('name'), user_data.get('screen_name')))
    tx.run("MERGE (tweet:Tweet {tweet_id: $tweet_id, score: $score, timestamp: $timestamp, text: $text, user_id:$user_id, name:$name, screen_name:$screen_name, image_url: $image_url, lang:$lang, source:$source})",
           tweet_id=tweet_id, score=score, timestamp=timestamp, text=text, user_id=user_data.get('id'), name=user_data.get('name'), screen_name=user_data.get('screen_name'), image_url=user_data.get('profile_image_url'), lang=lang, source=source)
    # print(tx)


def add_entity_tweet_relation(tx, date, topicName, sourceName, countryName, entityName, tweet_id):
    tx.run("Match (a:Entity {name: $entity})<-[r3:COUNTRY_ENTITY]-(c:Country {name: $country})<-[r1:SOURCE_COUNTRY]-(s:Source {name: $source})<-[r2:TOPIC_SOURCE]-(t:Topic{name: $topic})<-[rd:DATE_TOPIC]-(d:Date {date: $date}), (b:Tweet {tweet_id: $tweet_id}) "
           "SET a.totalMentions = a.totalMentions + 1,"
           "a.totalSentiment = a.totalSentiment + b.score,"
           "a.positiveSentiment = Case When b.score >0 Then a.positiveSentiment + b.score else a.positiveSentiment  End ,"
           "a.positiveMentions = Case When b.score >0 Then a.positiveMentions + 1 else a.positiveMentions End ,"
           "a.negativeSentiment = Case When b.score <0 Then a.negativeSentiment + b.score else a.negativeSentiment  End ,"
           "a.negativeMentions = Case When b.score <0 Then a.negativeMentions +1 else b.negativeMentions  End ,"
           "a.neutralMentions = Case When b.score =0 Then a.neutralMentions  +1 else b.neutralMentions End ,"
           "a.weakPositiveMentions = Case When b.score >0 Then Case when b.score <=0.30 Then a.weakPositiveMentions+1 else a.weakPositiveMentions End else a.weakPositiveMentions End ,"
           "a.mildPositiveMentions = Case When b.score >0.30 Then Case when b.score <=0.70 Then d.mildPositiveMentions+1 else d.mildPositiveMentions End else a.mildPositiveMentions End ,"
           "a.strongPositiveMentions = Case When b.score >0.70 Then a.strongPositiveMentions+1 else a.strongPositiveMentions End ,  "
           "a.weakNegativeMentions = Case When b.score <0 Then Case when b.score <=-0.30 Then d.weakNegativeMentions+1 else d.weakNegativeMentions End else a.weakNegativeMentions End ,"
           "a.mildNegativeMentions = Case When b.score <-0.30 Then Case when b.score >=-0.70 Then d.mildNegativeMentions+1 else d.mildNegativeMentions End else a.mildNegativeMentions End ,"
           "a.strongNegativeMentions = Case When b.score <-0.70 Then a.strongNegativeMentions+1 else a.strongNegativeMentions End "
           "CREATE (a)-[:ENTITY_TWEET]->(b)", date=date, entity=entityName, topic=topicName, source=sourceName, country=countryName, tweet_id=tweet_id)


def add_country_tweet_relation(tx, date, topicName, sourceName, countryName, tweet_id):
    tx.run("Match (a:Country {name: $country})<-[r1:SOURCE_COUNTRY]-(s:Source {name: $source})<-[r2:TOPIC_SOURCE]-(t:Topic{name: $topic})<-[rd:DATE_TOPIC]-(d:Date {date: $date}), (b:Tweet {tweet_id: $tweet_id}) "
           "CREATE (a)-[:COUNTRY_TWEET]->(b)", date=date, topic=topicName, source=sourceName, country=countryName, tweet_id=tweet_id)


def add_entity(tx, date, topicName, sourceName, countryName, entityName):
    tx.run("Match (c:Country {name: $country})<-[r1:SOURCE_COUNTRY]-(s:Source {name: $source})"
           "<-[r2:TOPIC_SOURCE]-(t:Topic{name: $topic})<-[rd:DATE_TOPIC]-(d:Date {date: $date}) "
           "MERGE (c)-[:COUNTRY_ENTITY]->(b:Entity {name: $entity}) "
           "ON CREATE set b.totalMentions=0, b.totalSentiment=0,"
           "b.positiveSentiment=0, b.positiveMentions=0,"
           "b.negativeSentiment=0, b.negativeMentions=0,"
           "b.strongNegativeMentions=0, b.mildNegativeMentions=0, b.weakNegativeMentions=0,"
           "b.strongPositiveMentions=0, b.mildPositiveMentions=0, b.weakPositiveMentions=0,"
           "b.neutralMentions=0 ",
           date=date, source=sourceName, country=countryName, topic=topicName, entity=entityName)


def add_country(tx, date, topicName, sourceName, countryName):
    tx.run("Match (d:Date {date: $date})-[rd:DATE_TOPIC]->(t:Topic {name: $topic})-[r1:TOPIC_SOURCE]->(a:Source {name: $source}) "
           "MERGE (a)-[:SOURCE_COUNTRY]->(b:Country {name: $country}) "
           "ON CREATE set b.totalMentions=0, b.totalSentiment=0,"
           "b.positiveSentiment=0, b.positiveMentions=0,"
           "b.negativeSentiment=0, b.negativeMentions=0,"
           "b.strongNegativeMentions=0, b.mildNegativeMentions=0, b.weakNegativeMentions=0,"
           "b.strongPositiveMentions=0, b.mildPositiveMentions=0, b.weakPositiveMentions=0,"
           "b.neutralMentions=0 ", date=date,
           source=sourceName, country=countryName, topic=topicName)

# def add_topic_source(tx, topicName, sourceName):
#     tx.run("MERGE (a:Topic {name: $name}) "
#             "ON CREATE set "
#             "a.totalMentions=0, a.totalSentiment=0,"
#             "a.positiveSentiment=0, a.positiveMentions=0,"
#             "a.negativeSentiment=0, a.negativeMentions=0,"
#             "a.neutralMentions=0 "
#             "MERGE (a)-[:TOPIC_SOURCE]->(b:Source {name: $source}) "
#             "ON CREATE set "
#             "b.totalMentions=0, b.totalSentiment=0,"
#             "b.positiveSentiment=0, b.positiveMentions=0,"
#             "b.negativeSentiment=0, b.negativeMentions=0,"
#             "b.neutralMentions=0 ",
#             name=topicName, source=sourceName)


def add_date_topic_source(tx, date_string, date_type, topicName, sourceName):
    tx.run("MERGE (date:Date {date: $date, type:$date_type}) "
           "ON CREATE set "
           "date.totalMentions=0, date.totalSentiment=0,"
           "date.positiveSentiment=0, date.positiveMentions=0,"
           "date.negativeSentiment=0, date.negativeMentions=0,"
           "date.strongNegativeMentions=0, date.mildNegativeMentions=0, date.weakNegativeMentions=0,"
           "date.strongPositiveMentions=0, date.mildPositiveMentions=0, date.weakPositiveMentions=0,"
           "date.neutralMentions=0 "
           "MERGE (date)-[:DATE_TOPIC]->(a:Topic {name: $topic}) "
           "ON CREATE set "
           "a.totalMentions=0, a.totalSentiment=0,"
           "a.positiveSentiment=0, a.positiveMentions=0,"
           "a.negativeSentiment=0, a.negativeMentions=0,"
           "a.strongNegativeMentions=0, a.mildNegativeMentions=0, a.weakNegativeMentions=0,"
           "a.strongPositiveMentions=0, a.mildPositiveMentions=0, a.weakPositiveMentions=0,"
           "a.neutralMentions=0 "
           "MERGE (a)-[:TOPIC_SOURCE]->(b:Source {name: $source}) "
           "ON CREATE set "
           "b.totalMentions=0, b.totalSentiment=0,"
           "b.positiveSentiment=0, b.positiveMentions=0,"
           "b.negativeSentiment=0, b.negativeMentions=0,"
           "b.strongNegativeMentions=0, b.mildNegativeMentions=0, b.weakNegativeMentions=0,"
           "b.strongPositiveMentions=0, b.mildPositiveMentions=0, b.weakPositiveMentions=0,"
           "b.neutralMentions=0 ",
           topic=topicName, source=sourceName, date=date_string, date_type=date_type)


def read_file_form_s3(bucket, filename):
    print("inside read_file_form_s3")
    print(filename)
    import boto3
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, filename)
    data = obj.get()['Body'].read().decode('utf-8')
    # print(data)
    return data.split('\n')


def insert_in_mongo(context_data):
    from pymongo import MongoClient
    # from pprint import pprint
    client = MongoClient("mongodb://localhost:27017/test")
    db = client.sentiment
    col = db.twitter
    col.insert_many(context_data)


event = {
    "Records": [
        {
            "messageId": "dc35200f-be2f-4378-93f4-4e1e606aaa70",
            "receiptHandle": "AQEBVDIux0GbLYs2+szro3bqGaHlfZVoA+AHyq281xvMYkoroEmv7fInUj5PCPZ4wgEhLgsZUdGxAJD+9QnZTGY7peHvw/yeMvWTr3KFGZGRe8mjpcZos7XJR3/jF2qYGYABHOz7wAjtjQzWq448LOKjEYNXxW5Ureg2PETziwO2K4jUajdtYavJcLumwTRaC+iGZFydnOrEDEzjsjFDwClVAjJWOdXAkM97bFvX/mqGCmoVkXoBPo9FuFxvlep4P+RqoR7Qs/v2cZiiQQQJxDNlrejHYZ8wqAT+zwyympNoW/RlH9AjzEfRNMlPRPI/sHrJas5q6LOykDYOXBgFlHQJXqqR2I1kOzqC37bK6GNCLh+iEf/toCrlWQvsLNQr8CaYavazcRlHGvZc4HVgVLZ7MJLdhWMAK0tAMSGf2EIQhd4=",
            "body": "{\"source\": \"twitter\", \"file\": \"analysed_bbc-newsSaudi Arabia.json\", \"text\": null}",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1552990384638",
                "SenderId": "AROAJNSSBBA3DYIAOTEII:lmb-use1-sentiment--twitter-data-ingestion",
                "ApproximateFirstReceiveTimestamp": "1552990394638"
            },
            "messageAttributes": {
                "WeeksOn": {
                    "stringValue": "6",
                    "stringListValues": [],
                    "binaryListValues": [],
                    "dataType": "Number"
                },
                "Author": {
                    "stringValue": "Dev Team",
                    "stringListValues": [],
                    "binaryListValues": [],
                    "dataType": "String"
                },
                "Title": {
                    "stringValue": "Sentiment Analysis",
                    "stringListValues": [],
                    "binaryListValues": [],
                    "dataType": "String"
                }
            },
            "md5OfMessageAttributes": "25fe00b4ee984a6fba20ee5356ce83fd",
            "md5OfBody": "ab81fd3c8eb931789a1871d2ba6b9a7d",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:069653090426:sqs-use1-sentiment-analysis",
            "awsRegion": "us-east-1"
        }
    ]
}


countries = {
    "Israel": "israel",
    "Nigeria": "nigeria",
    "Dallas, TX": "usa",
    "saudi": "saudi",
    "empty": "other",
    "uk": "uk",
    "Dubai United Arab Emirates": "saudi",
    "Jubail": "saudi",
    "Ontario, Canada": "canada",
    "Riyadh, SA": "saudi",
    "Saudi Arabia": "saudi",
    "usa": "usa",
    "Riyadh": "saudi",
    "Brussels, Belgium": "belgium",
    "Toronto, Ontario": "canada",
    "Koninginnegracht 26, The Hague": "netherland",
    "United Arab Emirates": "saudi",
    "Tarcento": "itly",
    "Singapore": "singapore",
    "Desert View": "israel",
    "Jeddah / Jeddah": "saudi",
    "Washington, IL": "usa",
    "Ala": "other",
    "Wichita": "usa",
    "KSA | UK": "uk",
    "Ksa Riyadh": "saudi",
    "Jeddah": "saudi",
    "Assam, India": "india",
    "KSA Riyadh": "saudi",
    "United States": "usa",
    "Dubai, United Arab Emirates": "saudi",
    "Regina SK, Canada": "canada",
    "Odessa TX": "ukrain",
    "USA": "usa",
    "South Australia, Australia": "australia",
    "Gaffney, SC": "ireland",
    "Wellington, NZ": "newzealand",
    "Kenya": "kenya",
    "Pakistan": "pakistan",
    "East, England": "uk",
    "Opal Tower, Business Bay Dubai": "saudi",
    "Finland": "finland",
    "Dubai": "saudi",
    "London, UK": "uk",
    "Jalandhar, India": "india",
    "Washington | Berlin": "usa",
    "Croy, Scotland": "scotland",
    "Ontario": "canada",
    "City of London, London": "uk",
    "Indonesia": "indonesia",
    "Khafji": "saudi",
    "Norway": "norway",
    "Wales, United Kingdom": "uk",
    "Riyadh, SA https://goo.gl/maps": "saudi",
    "NYC": "usa",
    "Moscow, Russia": "russia",
    "Rochdale, England": "uk",
    "Bahrain": "bahrain",
    "Cairo, Egypt": "egypt",
    "Islamic Republic of Iran": "iran",
    "Dublin City, Ireland": "ireland",
    "IRAN": "iran",
    "Germany": "germany",
    "London UK": "uk",
    "Mumbai, India": "india",
    "Rotterdam, the Netherlands": "netherland",
    "Madrid": "spain",
    "Dhahran": "nepal",
    "London": "uk",
    "Glenrothes": "scotland",
    "England, United Kingdom": "uk",
    "The Hague, South Holland": "holland",
    "Scotland": "scotland",
    "Qassim KSA": "saudi",
    "New York": "usa",
    "Paris, France": "france",
    "Peshawar Pakistan": "pakistan",
    "The Kremlin": "russia",
    "United Kingdom": "uk",
    "Helsinki, Finland": "finland",
    "Delhi India": "india",
    "Houston": "usa",
    "Lincoln, England": "uk",
    "Imam Saud Ibn Abdul Aziz Road, Wady Al Muaydin Street, Unit 4, An Nakhil, Riyadh": "saudi",
    "Scotland, United Kingdom": "uk",
    "Greater Vancouver A": "canada",
    "Karachi Pakistan": "pakistan",
    "Belgorod, Russia": "russia",
    "Montreal, Quebec": "canada",
    "Beirut": "lebanon",
    "Abuja, Nigeria": "nigeria",
    "Bradford, England": "uk",
    "Westmeath, Ireland": "ireland",
    "Egypt,Alexandria": "egypt",
    "Australia": "australia",
    "Karachi, Pakistan": "pakistan",
    "Canada": "canada",
    "Vancouver, BC, Canada": "canada",
    "Helsingborg, Skåne Sweden": "sweden",
    "Islamabad, Pakistan": "pakistan",
    "Berlin, Germany": "germany",
    "Abuja": "nigeria",
    "Wollongong, New South Wales": "uk",
    "Hyderabad, India": "india",
    "Riyadh , Amman": "saudi",
    "Riyadh, Saudi Arabia": "saudi",
    "New York, USA": "usa",
    "kingdom of Saudi": "saudi",
    "Kingdom of Saudi Arabia": "saudi",
    "Eastern, Kingdom of Saudi Arabia": "saudi",
    "Riyadh, Kingdom of Saudi Arabia": "saudi",
    "Alkhobar, Saudi Arabia": "saudi",
    "Makkah, Saudi Arabia": "saudi",
    "Al-Khobar, Kingdom of Saudi Arabia": "saudi",
    "Jeddah-Saudi Arabia": "saudi",
    "Great Saudi Arabia": "saudi"
}
source, topic, filename = (argv[1], argv[2], argv[3])
lambda_handler(source, topic, filename)
