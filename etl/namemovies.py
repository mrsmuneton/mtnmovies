import pandas as pd
from redis import Redis

print("getting movie names")
r = Redis(host = 'redis', port = 6379)
movies = pd.read_csv("./ml-25m/movies.csv", sep = ',')
for row in movies.iterrows():
    r.set(row[1][0],row[1][1])
