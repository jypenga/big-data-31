import os
import ray
import time
import math
import duckdb
import unidecode

import numpy as np
import pandas as pd

from workers.columns import column_cleaner, column_cleaner_avg, column_cleaner_nan_avg, column_cleaner_R
from workers.tables import table_transformer


# cleaning text-like values
def f(x):
    if isinstance(x, str):
        return unidecode.unidecode(x)
    else:
        return ''


if __name__ == '__main__':
    t0 = time.time()
    db = os.path.join('db', 'unstructured.duckdb')

    if os.path.exists(os.path.join('db', 'structured.duckdb')):
        os.remove(os.path.join('db', 'structured.duckdb'))

    # init resources
    ray.init(num_cpus=4)

    # transform external data
    p1 = table_transformer.remote(db,
                                  'mtRatings',
                                  'mtRatings',
                                  """SELECT tconst,
                                  avg(rating) AS mtRatings
                                  FROM mt_ratings 
                                  GROUP BY tconst""")

    p2 = table_transformer.remote(db,
                                  'mlRatings',
                                  'mlRatings',
                                  """SELECT ml_links.imdbId AS tconst,
                                  avg(ml_ratings.rating) AS mlRatings
                                  FROM ml_ratings 
                                  INNER JOIN ml_links 
                                  ON ml_ratings.movieId = ml_links.movieId 
                                  GROUP BY ml_links.imdbId""")

    # clean and separate feature and label columns
    p3 = column_cleaner_avg.remote(db, 'startYear')
    p4 = column_cleaner_avg.remote(db, 'endYear')
    p5 = column_cleaner_avg.remote(db, 'runtimeMinutes')
    p6 = column_cleaner_nan_avg.remote(db, 'numVotes')
    p7 = column_cleaner.remote(db, 'originalTitle', f)
    p8 = column_cleaner.remote(db, 'primaryTitle', f)
    p9 = column_cleaner.remote(db, 'label', tables=['train'])

    # R cleaners
    p10 = column_cleaner_R.remote('writers.R')
    p11 = column_cleaner_R.remote('directors.R')

    *results, _, _ = ray.get([p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11])
    ray.shutdown()

    # refactor results
    names, dfs = [], []
    for result in results:
        name, df = result
        names += name
        dfs += df

    # dump all
    conn = duckdb.connect(os.path.join('db', 'structured.duckdb'), read_only=False)
    
    for name, df in zip(names, dfs):
        conn.register(name, df)
        conn.execute(f'CREATE TABLE {name} AS SELECT * FROM {name}')

    for fname in os.listdir('R'):
        if fname.endswith('writers.csv') or fname.endswith('directors.csv'):
            name = fname.split('.')[0]
            df = pd.read_csv(os.path.join('R', fname))
            conn.register(name, df)
            conn.execute(f'CREATE TABLE {name} AS SELECT * FROM {name}')
            os.remove(os.path.join('R', fname))
        
    conn.close()
    print(f'runtime: {round(time.time() - t0, 2)}s')
