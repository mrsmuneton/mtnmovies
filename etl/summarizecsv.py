import csv
from redis import Redis

def writesummary(filepath, dataset):
    with open(filepath, 'w', newline='') as csvfile:
        fh = csv.writer(csvfile, delimiter=",")
        for row in dataset:
            movieid = row[0].decode('utf-8').replace('m-','')
            name = r.get(movieid).decode('utf-8')
            print(f"writing {name} count {row[1]}")
            fh.writerow([name, row[1]])

r = Redis(host = 'redis', port = 6379)

alltime  = r.zrange('movies', 0, 20, desc=True, withscores=True)
writesummary('/etl/summaries/alltime.csv', alltime)

batches = r.keys("batch*")
for batch in batches:
    xbatchcounter = "x-" + batch.decode('utf-8')
    thisbatch = r.zrange(xbatchcounter, 0, 20, desc=True, withscores=True)
    writesummary("/etl/summaries/" + batch.decode('utf-8') + ".csv", thisbatch)
