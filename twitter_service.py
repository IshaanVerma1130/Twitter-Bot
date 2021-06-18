import tweepy
import json
import operator
import re
from rake_nltk import Rake
from nltk.corpus import stopwords
from decouple import config
from queue import Queue
import mysql.connector as sqlc
import threading
import time
import pymongo

# Termanination condition message.
# The admin can press 'enter' to stop the program.
print("Press 'Enter' key to stop the program.")
run = True

# Connection to MySQL Database
try:
  sql_db = sqlc.connect(host=config('host'),
                        user=config('user'),
                        password=config('password'))
  print('Connected to MySQL database.')

except sqlc.Error as err:
  print(f"Error while connecting to MySQL database: {err}")

# Setting the starting conditions
# for the user_list table in MySQL DB:
# Make the value of status column in every row = 0
cur = sql_db.cursor()
cur.execute('USE users')
cur.execute('UPDATE user_list SET status = 0 WHERE status = 1')

# Connection to Mongo DB
try:
  client = pymongo.MongoClient(
      f'mongodb+srv://Ishaan:{config("mongo_pass")}@cluster0.dnavy.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
  )
  print('Connected to Mongo database.')
except:
  print('Error while connecting to Mongo database.')

# Connect to 'twitter' collection in 'twitter' DB
mongo_db = client['twitter']
collection = mongo_db['twitter']

# Checking if a document already exists in the collection
# If not then initialize the
# 'url_list', 'topics' and 'frequencies' documents
try:
  count = collection.find({})
  if len(list(count)) == 0:
    collection.insert_one({'url_list': [], 'topics': [], 'frequencies': {}})
  print('Initialization of Mongo DB finished.')

except:
  ('Error while initializing documents in Mongo collection.')

# Twitter Auth connection setup
auth = tweepy.OAuthHandler(config('consumer_key'), config('consumer_secret'))
auth.set_access_token(config('access_token'), config('access_secret'))
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

try:
  api.verify_credentials()
  print("Twitter authentication successful.")
except:
  print("Error during authenticating twitter credentials.")


# Remove emojis from text
def deEmojify(text):
  regrex_pattern = re.compile(
      pattern="["
      u"\U0001F600-\U0001F64F"  # emoticons
      u"\U0001F300-\U0001F5FF"  # symbols & pictographs
      u"\U0001F680-\U0001F6FF"  # transport & map symbols
      u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
      "]+",
      flags=re.UNICODE)
  return regrex_pattern.sub(r'', text)


# Keyword extraction function for a given text input
def extract(text: 'str') -> dict:
  ''' Function implemented using Rake for keyword extraction.
      Extract keywords along with their frequencies.'''

  r = Rake()
  r.extract_keywords_from_text(text)
  words = dict(r.get_ranked_phrases_with_scores())
  word_degrees = dict(r.get_word_degrees())
  freq = r.get_word_frequency_distribution()
  word_freq = {}
  for key, value in word_degrees.items():
    if value > 1:
      word_freq[key] = freq[key]
  return word_freq


# Global counter 'i' for row number in database
i = 1


# Function for fetching MySQL data row wise
def fetch():
  '''Fetch usernames row wise from MySQL database and put in Queue.'''
  global i, run
  print('Running MySQl queries.')

  def update():
    '''Update status for i'th row in MySQL database to 1. This will lock the i'th row.'''
    try:
      cur.execute('UPDATE user_list SET status = 1 WHERE ID = %s', (i, ))
      print(i)
    except sqlc.Error as e:
      print(f'Error while locking row in MySQL database: {e}')

  def send():
    '''Retrieve username of an entry in MySQL database where status is equal to 1'''
    try:
      cur.execute('SELECT username FROM user_list WHERE status = 1')
      name = cur.fetchone()
      cur.execute('UPDATE user_list SET status = 0 WHERE ID = %s ', (i, ))
      if name:
        name = name[0]
      return name
    except sqlc.Error as e:
      print(f'Error while fetching username from MySQL database: {e}')

  while True:
    if not run:
      print('Stopped fetching from MySQL DB!')
      return
    update()
    username = send()
    if username:
      i += 1
      q.put(username)
      time.sleep(2)


# Function for inserting processed data in MongoDB
def send_mongo():
  '''Send processed data to Mongo DB.'''
  global run

  def insert(name: 'str'):
    '''Insert urls and keywords with frequencies into Mongo DB.'''

    # Fetch public info about a user using Twitter API
    status = []
    try:
      print(name)
      status = api.user_timeline(screen_name=name,
                                 count=1,
                                 include_rts=False,
                                 tweet_mode='extended')

    except tweepy.TweepError as ex:
      if ex.reason == "Not authorized.":
        print(f'User "{name}" not authorized. Continuing')
        return

    if len(status) == 0:
      return

    tweet_text = status[0]['full_text']

    # Remove 'http' link, emojis and '@' tags from text
    tweet_text = re.sub(r'http\S+', '', tweet_text)
    tweet_text = re.sub(r'@\S+', '', tweet_text)
    tweet_text = deEmojify(tweet_text)

    url_list = status[0]['entities']['urls']

    keywords_freq = extract(tweet_text)  # Extracted keywords (dict)
    urls = []  # List of urls

    for item in url_list:
      urls.append(item['expanded_url'])

    # Add urls to Mongo DB
    for url in urls:
      collection.update_one({}, {"$addToSet": {"url_list": url}})

    # Add keyword frequencies to Mongo DB
    if bool(keywords_freq):
      time.sleep(1)
      for keyword in keywords_freq:
        try:
          collection.update_one({}, {
              "$addToSet": {
                  "topics": keyword
              },
              "$inc": {
                  f"frequencies.{keyword}": keywords_freq[keyword]
              }
          })
        except:
          continue

  while True:
    if not run:
      return
    if not q.empty():
      insert(q.get())
      time.sleep(0.5)


# Fucntion for termating the program
def quit():
  '''Helper function to stop program execution.'''
  global run
  input()
  run = False


# Queue which enables us to communicate between threads
q = Queue()

# Thread 1 for fetching data from MySQl DB
thd_fetch = threading.Thread(target=fetch, args=())
thd_fetch.start()

# Thread 2 for sending data to Mongo DB
thd_send_mongo = threading.Thread(target=send_mongo, args=())
thd_send_mongo.start()

# Thread 3 for terminating the program
thd_quit = threading.Thread(target=quit, args=())
thd_quit.start()
