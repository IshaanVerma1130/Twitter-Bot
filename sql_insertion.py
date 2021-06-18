import mysql.connector as sqlc
from decouple import config
import tweepy
import random
import time

# Connection to MySQL Database
try:
  db = sqlc.connect(host=config('host'),
                    user=config('user'),
                    password=config('password'))
  print('Connected to MySQL database.')

except sqlc.Error as err:
  print(f"Something went wrong: {err}")

cur = db.cursor()

# Setting the starting conditions MySQL DB:
# 1. Create 'users' database if it doesnot exist
# 2. Create 'user_list' table if it doesnot exist with 'ID', 'username', 'status' columns
initialization_queries = [
    'CREATE DATABASE IF NOT EXISTS users', 'USE users', '''
  CREATE TABLE IF NOT EXISTS user_list(
  ID INT(11) NOT NULL UNIQUE,
  username VARCHAR(50) NOT NULL UNIQUE,
  status INT(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (ID)
  )
  '''
]

# Initializing Database
for query in initialization_queries:
  cur.execute(query)

# Twitter Auth connection setup
auth = tweepy.OAuthHandler(config('consumer_key'), config('consumer_secret'))
auth.set_access_token(config('access_token'), config('access_secret'))
api = tweepy.API(auth)

try:
  api.verify_credentials()
  print("Twitter authentication successful.")
except:
  print("Error during authenticating twitter credentials.")

# Twitter handles with a large amount of followers
handles = ['justinbieber', 'BarackObama', 'katyperry', 'rihanna', 'Cristiano']

try:
  cur.execute('SELECT MAX(ID) FROM user_list')

  # Global counter for i'th row in MySQL DB
  i = cur.fetchone()
  if i[0]:
    i = i[0] + 1
  else:
    i = 1

  # Get follower list for any handle from the 'handles' list
  # for user in api.followers(random.choice(handles)):
  for user in tweepy.Cursor(api.followers,
                            screen_name=random.choice(handles),
                            count=50).items():
    try:
      print(user.screen_name)
      # Inserting every user into MySQL DB
      cur.execute("INSERT IGNORE INTO user_list(ID, username) VALUES (%s, %s)",
                  (i, user.screen_name))

      # Commit to MySQL DB for every 10th user
      if i % 10 == 0:
        db.commit()
        time.sleep(1)
      i += 1

    except sqlc.Error as e:
      print(f"Error while inserting into MySQL database: {e}")

  print('All users have been inserted into the database.')

except:
  print('Exhausted limit for Twitter API calls.')

db.commit()

# Close off cursor and MySQL DB connection
cur.close()
db.close()
print('MySQL connection has been closed.')
