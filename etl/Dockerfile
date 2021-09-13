FROM python:3.8
ADD . /etl
WORKDIR /etl

RUN pip install -r requirements.txt

CMD python fetchdata.py && \
  python namemovies.py && \
  python partition.py && \
  python batchprocess.py && \
  python summarizecsv.py
