# compose_flask/app.py
from flask import Flask, render_template
from redis import Redis

app = Flask(__name__)
r = Redis(host='redis', port=6379)

def movieset(set):
    movies = []
    for list in set:
        movieid = list[0].decode('utf-8').replace('m-','')
        name = r.get(movieid)
        movies.append(name.decode('utf-8') + " with " + str(int(list[1])) + " extreme ratings")
    return movies

@app.route('/')
def hello():
    alltime  = r.zrange('movies', 0, 20, desc=True, withscores=True)
    try:
        movies = movieset(alltime)
        batches = r.keys("batch*")
        return render_template('display.html', batches=batches, len = len(batches), movies=movies, movieslen = len(movies))
    except:
        return render_template('display.html', batches=[], len = len([]), movies=movies, movieslen = len(movies))

@app.route('/batch/<batch>')
def batch(batch=None):
    xbatchcounter = "x-" + batch
    thisbatch = r.zrange(xbatchcounter, 0, 20, desc=True, withscores=True)
    movies = movieset(thisbatch)
    return render_template('batch.html', batch=batch, movies=movies, movieslen = len(movies))

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, use_reloader = True)
