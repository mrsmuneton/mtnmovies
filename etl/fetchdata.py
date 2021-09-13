from urllib.request import urlopen
from zipfile import ZipFile

zipresp = urlopen("https://files.grouplens.org/datasets/movielens/ml-25m.zip")

fh = open("/etl/ml-25m.zip", "wb")
fh.write(zipresp.read())
fh.close()

zf = ZipFile("/etl/ml-25m.zip")
zf.extractall(path= "/etl")
zf.close()
