import os
import pandas as pd

in_csv = "/etl/ml-25m/ratings.csv"
#get the number of lines of the csv file to be read
number_lines = sum(1 for row in (open(in_csv)))

rowsize = 100000
for i in range(1, number_lines, rowsize):
    df = pd.read_csv(in_csv,
          header=None,
          nrows = rowsize,
          skiprows = i)#skip rows that have been read
    out_csv = "/etl/ratings_files/batch" + str(i) + ".csv"
    if os.path.exists(out_csv):
        print("this file exists on disk, skipping")
    else:
        print(f'writing out csv {out_csv}')
        df.to_csv(out_csv,
              index=False,
              header=False,
              mode="a",
              chunksize=rowsize)#size of data to append for each loop

    #timebox cheating, limit to a few partitions
    if 3 * (100000) + 1 < i :
        print(f"breaking out of partitioning full set at ~row {i}")
        break;
