import csv
from redis import Redis
import os
import pandas as pd

r = Redis(host = 'redis', port = 6379)
for csvfile in os.listdir("/etl/ratings_files"):
    batch = csvfile.replace(".csv","")
    completed = r.get(batch)
    if "batch" not in csvfile:
        continue
    if r.get(batch) == "completed".encode("utf-8"):
        print("This batch has been processed, skipping")
        continue

    ratings = pd.read_csv("/etl/ratings_files/" + csvfile, sep = ',')
    r.set(batch,"started")

    print(f'writing batch {batch} to redis')
    for row in ratings.iterrows():
        # timestamp = row[1][3]
        # to comprehend the timestamp (is it earliest appearance, within two weeks of?)
        # I will need to read, not implemented in this stage
        # one approach to find first 14 days ratings include creating a movie key with all
        # xtreme timestamps observed and then in the next stage,
        # sort and collate to determine earliest occurance
        # (maybe truncate data beyond target window at this point?)
        # sum timestamp count over our 14 days
        #
        # in that case, LPUSH movieid timestamp in this stage

        rating = int(row[1][2])
        movieid = "m-" + str(int(row[1][1]))
        xbatchcounter = "x-" + batch

        # increment counters
        if rating in [0,1,5]:
            r.zincrby("movies", 1, movieid)
            r.zincrby(xbatchcounter, 1, movieid)

    r.set(batch,"completed")
