import os
import math
import duckdb
import unidecode

import numpy as np

from workers.columns import column_cleaner
from workers.tables import table_transformer


def store(result):
    global names, dfs
    name, df = result
    names += name
    dfs += df


def connect():
    return duckdb.connect(os.path.join('db', 'unstructured.duckdb'), read_only=True)


if __name__ == '__main__':
    if os.path.exists(os.path.join('db', 'structured.duckdb')):
        os.remove(os.path.join('db', 'structured.duckdb'))

    names, dfs = [], []

    # cleaning numerical values
    def cleaner(x, filler=0):
        if x == r'\N':
            return int(filler)
        else:
            return int(x)

    store(column_cleaner(connect(), 'startYear', lambda x: cleaner(x)))
    store(column_cleaner(connect(), 'endYear', cleaner))
    store(column_cleaner(connect(), 'runtimeMinutes', cleaner))

    def cleaner(x):
        if x == 'nan' or x == "NaN" or math.isnan(x):
            return int()
        else:
            return int(x)

    store(column_cleaner(connect(), 'numVotes', cleaner))

    # cleaning text-like values
    def cleaner(x):
        if isinstance(x, str):
            return unidecode.unidecode(x)
        else:
            return ''

    store(column_cleaner(connect(), 'originalTitle', cleaner))
    store(column_cleaner(connect(), 'primaryTitle', cleaner))

    # transforming labels
    store(column_cleaner(connect(), 'label', tables=['train']))

    # transforming external data
    store(table_transformer(connect(), 
                            'mtRatings',
                            'mtRatings',
                            """SELECT tconst,
                            avg(rating) AS mtRatings
                            FROM mt_ratings 
                            GROUP BY tconst"""))

    store(table_transformer(connect(), 
                            'mlRatings',
                            'mlRatings',
                            """SELECT ml_links.imdbId AS tconst,
                            avg(ml_ratings.rating) AS mlRatings
                            FROM ml_ratings 
                            INNER JOIN ml_links 
                            ON ml_ratings.movieId = ml_links.movieId 
                            GROUP BY ml_links.imdbId"""))

    # dump all
    conn = duckdb.connect(os.path.join('db', 'structured.duckdb'), read_only=False)
    
    for name, df in zip(names, dfs):
        conn.register(name, df)
        conn.execute(f'CREATE TABLE {name} AS SELECT * FROM {name}')

    conn.close()
