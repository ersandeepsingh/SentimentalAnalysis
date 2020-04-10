from neo4j import GraphDatabase
from googletrans import Translator
translator = Translator()
from dateutil.parser import parse
from sys import argv
# driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "qwerty123456"))
driver = GraphDatabase.driver("bolt://ec2-3-94-239-53.compute-1.amazonaws.com:7687", auth=("neo4j", "i-02eb90c960f982751"))
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




def lambda_handler(source, topic, filename, countries):
    import json
    import config

    context_data = read_file_form_s3(config.S3_BUCKET, filename)
    try:
        result = [json.loads(item) for item in context_data]
        print("number of analysed tweets " , len(result))
        for index, data in enumerate(result):

            with driver.session() as session:
                date = parse(data['created_at'])
                date_string = "{:%Y-%m-%d}".format(date)
                date_type = 'daily'

                # code for date aggregation
                run_query(session, add_date_topic_source,
                          date_string, date_type, topic, source)
                sentiment = data['analysis']['sentiment']['documentSentiment']
                score = sentiment.get('score') if sentiment.get('score') else 0
                tweet_id = data['id']
                text = data["retweeted_status"]["extended_tweet"]["full_text"] 
                tweet_created_at = data.get('created_at')
                user_data = data.get('user')
                lang = data.get('lang')
                print("tweet_id  ::::::: " ,tweet_id  )
                if not session.read_transaction(isTweetExist, text):
                    run_query(session, add_tweet, tweet_id, score, tweet_created_at, text, user_data, lang,"twitter")
                    print("tweet added")
                    try:
                        print("loc")
                        try:
                            loc = data['user']['location'].strip().lower() if data['user']['location'] else 'other'
                            print("loc---{}----".format(loc))
                            # loc = translator.translate(loc).text
                        
                            print("loc", loc)
                            loc = 'uk' if loc.find('uk') > 0 else loc
                            loc = loc.split(' ')[-1]
                            loc = countries.get(loc) if countries.get(
                                loc) is not None else loc
                            print("loc", loc)
                        except Exception as e:
                            print("location is having some problem.", loc)
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

#text duplication removable for retweet
#TO DO: will be changed on future requirements
def isTweetExist(tx, text):
    id =None
    for tweet in tx.run("Match(tt:Tweet {text:$text}) Return tt.tweet_id", text=text):
        print(tweet['tt.tweet_id'])
        id = tweet['tt.tweet_id']
        break
    print('TweetExist already exist with id::::',  id)
    return id

def update_all_count(tx, date, topicName, sourceName, countryName, tweet_id):
    tx.run(update_count_query, date=date, topic=topicName,
           source=sourceName, country=countryName, tweet_id=tweet_id)


def add_tweet(tx, tweet_id, score, timestamp, text, user_data, lang, source):
    try:
        print("tweet_id  " , tweet_id , "  score ", score, "  timestamp ", timestamp )
        print("  lang ", lang)
        print("adding tweet {} {} {} {} ".format(tweet_id, score,
                                             user_data.get('name'), user_data.get('screen_name')))
        tx.run("MERGE (tweet:Tweet {tweet_id: $tweet_id, score: $score, timestamp: $timestamp, text: $text, user_id:$user_id, name:$name, screen_name:$screen_name, image_url: $image_url, lang:$lang, source:$source})",
           tweet_id=tweet_id, score=score, timestamp=timestamp, text=text, user_id=user_data.get('id'), name=user_data.get('name'), screen_name=user_data.get('screen_name'), image_url=user_data.get('profile_image_url'), lang=lang, source=source)
        # print(tx)
    except Exception as e:
        print("error in adding tweets", e)


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
    print("adding tweet to country relationship")
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
    print("file read complete")
    return data.split('\n')


def insert_in_mongo(context_data):
    from pymongo import MongoClient
    # from pprint import pprint
    client = MongoClient("mongodb://localhost:27017/test")
    db = client.sentiment
    col = db.twitter
    col.insert_many(context_data)

countries = {
  "israel": "israel",
  "nigeria": "nigeria",
  "dallas, tx": "usa",
  "saudi": "saudi",
  "empty": "other",
  "dubai united arab emirates": "saudi",
  "jubail": "saudi",
  "ontario, canada": "canada",
  "riyadh, sa": "saudi",
  "saudi arabia": "saudi",
  "usa": "usa",
  "riyadh": "saudi",
  "brussels, belgium": "germany",
  "toronto, ontario": "canada",
  "koninginnegracht 26, the hague": "netherland",
  "united arab emirates": "saudi",
  "tarcento": "itly",
  "singapore": "singapore",
  "desert view": "israel",
  "jeddah / jeddah": "saudi",
  "washington, il": "usa",
  "ksa | uk": "uk",
  "ksa riyadh": "saudi",
  "assam, india": "india",
  "united states": "usa",
  "dubai, united arab emirates": "saudi",
  "regina sk, canada": "canada",
  "south australia, australia": "australia",
  "gaffney, sc": "usa",
  "wellington, nz": "usa",
  "kenya": "kenya",
  "pakistan": "pakistan",
  "east, england": "uk",
  "opal tower, business bay dubai": "saudi",
  "finland": "finland",
  "dubai": "saudi",
  "london, uk": "uk",
  "jalandhar, india": "india",
  "washington | berlin": "germany",
  "croy, scotland": "scotland",
  "ontario": "canada",
  "city of london, london": "uk",
  "indonesia": "indonesia",
  "khafji": "saudi",
  "norway": "norway",
  "wales, united kingdom": "uk",
  "riyadh, sa https://goo.gl/maps": "saudi",
  "nyc": "usa",
  "moscow, russia": "russia",
  "rochdale, england": "uk",
  "bahrain": "saudi",
  "cairo, egypt": "egypt",
  "islamic republic of iran": "iran",
  "dublin city, ireland": "uk",
  "iran": "iran",
  "germany": "germany",
  "london uk": "uk",
  "mumbai, india": "india",
  "rotterdam, the netherlands": "netherland",
  "madrid": "spain",
  "dhahran": "saudi",
  "london": "uk",
  "glenrothes": "scotland",
  "england, united kingdom": "uk",
  "the hague, south holland": "holland",
  "scotland": "scotland",
  "qassim ksa": "saudi",
  "new york": "usa",
  "paris, france": "france",
  "peshawar pakistan": "pakistan",
  "the kremlin": "russia",
  "united kingdom": "uk",
  "helsinki, finland": "finland",
  "delhi india": "india",
  "houston": "usa",
  "lincoln, england": "uk",
  "scotland, united kingdom": "uk",
  "greater vancouver a": "canada",
  "karachi pakistan": "pakistan",
  "belgorod, russia": "russia",
  "montreal, quebec": "canada",
  "bradford, england": "uk",
  "westmeath, ireland": "uk",
  "australia": "australia",
  "karachi, pakistan": "pakistan",
  "canada": "canada",
  "vancouver, bc, canada": "canada",
  "helsingborg, skåne sweden": "sweden",
  "islamabad, pakistan": "pakistan",
  "berlin, germany": "germany",
  "abuja": "nigeria",
  "wollongong, new south wales": "uk",
  "hyderabad, india": "india",
  "riyadh , amman": "saudi",
  "": "other",
  " ": "other",
  "arizona, usa": "usa",
  "austria": "austria",
  "belgië belgium belgique": "germany",
  "berlin and jerusalem": "germany",
  "berlin based nomad": "germany",
  "birmingham uk ": "uk",
  "bruxelles, belgique": "germany",
  "buckinghamshire": "uk",
  "ct usa": "usa",
  "camden, me": "uk",
  "edinburgh": "scotland",
  "europe,bulgaria,varna": "bulgaria",
  "fortaleza, brazil": "brazil",
  "fremont, ca us": "usa",
  "geneva, switzerland": "europe",
  "granada, españa": "europe",
  "gujrat, pakistan": "pakistan",
  "halifax, england": "uk",
  "houston, texas. usa": "usa",
  "izmir, turkey": "turkey",
  "jeddah, kingdom of saudi arabia": "suadi",
  "jeddah, saudi arabia": "saudi",
  "jeddah;saudi arabia-poland": "saudi",
  "jerusalem london ": "uk",
  "ksa | uk ": "uk",
  "karachi pakistan ": "pakistan",
  "kingdom of saudi arabia": "saudi",
  "kingdom of saudi arabia ": "saudi",
  "kolkata, india": "india",
  "lahore pakistan ": "pakistan",
  "lahore, pakistan": "pakistan",
  "london ": "england",
  "london, england": "uk",
  "madina": "saudi",
  "makkah, saudi arabia": "saudi",
  "manama, bahrain": "saudi",
  "montréal, québec": "canada",
  "morocco": "saudi",
  "moskau": "russia",
  "new york, usa": "usa",
  "ohio, usa": "usa",
  "peshawar pakistan ": "pakistan",
  "prestwick, scotland": "scotland",
  "puerto rico, usa": "usa",
  "qassim, buraydah.": "saudi",
  "riyad, arabia saudita": "saudi",
  "riyadh , saudi arabia": "saudi",
  "riyadh, kingdom of saudi arabia": "saudi",
  "riyadh, saudi arabia": "saudi",
  "riyadh,sa": "saudi",
  "saudia arabia ": "saudi",
  "tokyo, japan ": "japan",
  "vienna, austria": "austria",
  "washington, dc": "usa",
  "New Delhi, India" : "india",
  "USA" : "usa",
  "england" : "uk",
  "london, england": "uk",
  "melbourne, victoria" : "australia", 
  "victoria" : "uk",
  "06.70.83.32.58": "other",
  "oxford" : "uk",
  "glasgow" : "uk",
  "essex" : "uk",
  "kingdom" : "uk",
  "scotland" : "uk",
  "canada" : "canada",
  "ireland" : "uk",
  "mostly" : "other",
  "wirral" : "uk",
  "au" : "other",
  "spain" : "spain",
  "chester,england" : "uk",
  "mois" : "other",
  "...." :  "other",
  "tx" : "other",
  "is": "other",
"zealand" : "other",
"europe" : "other",
"britain" : "uk",
"states" : "other",
"mind" : "other",
"yorks" : "uk",
"notneoliberalshittown" : "uk",
"brasil" : "brazil",
"coldfield" : "uk",
"derbyshire" : "uk",
"avalon" : "other",
"(usually)." : "other",
"france" : "france",
"usa" : "usa",
"where" : "other",
"wa" : "other",
"gomorrah" : "other",
"Delhi India" : "india",
"planet" : "other",
"bc" : "other",
"dorset" : "uk",
"city" : "other",
"lincolnshire" : "uk",
"scotland." : "uk",
"wales" : "uk",
":(" : "uk",
"gb" : "uk",
"leith" : "uk",
"england." : "uk",
"cardiff" : "uk",
"world" : "other",
"earth" : "other",
"cymru" : "other",
"u.k." : "uk",
"kentucky" : "uk",
"smethwick." : "uk",
"eu" : "uk",
"union" : "uk",
"heath/mechelen" : "other", 
"surrey": "other",

"mushin": "japan",
"tanzania" : "tanzania",
"schweiz" : "switzerland",
"berlin" : "germany",
"österreich" : "other",
"latium" : "other",
"mi" : "other",
"india." : "india",
"nigeria" : "nigeria", 
"españa" : "other",
"ulaanbaatar" : "other",
"futurist" : "other",
"sdg16" : "usa",
"ny" : "usa",
"ca" : "usa",
"america" : "usa",
"vermont" : "other",
"q-rah" : "other",
"espanya" : "other",
"lumpur" : "other",
"tasmania" : "other",
"or" : "other",
"united states" : "usa",
"florida" : "usa",
"#amerikkka" : "usa",
"polska" : "usa",
"info[at]impakter.com" : "other",
"tumblri" : "usa",
"adelaide" : "australia",
"portharcourt" :  "nigeria",
"rica" : "brazil",
"international" : "other",
"global" : "other",
"pluto" : "other",
"louis" : "other",
"are." : "other",
"chi" : "other",
"ghana" : "africa",
"impact" : "other",
"francisco" : "spain",
"accra" : "other",
"lawyer" : "other",
"ec" : "other",
"republic" : "other",
"cambridge" : "uk",
"tokyo" : "japan"
}

source, topic, filename = (argv[1], argv[2], argv[3])
lambda_handler(source, topic, filename, countries)
