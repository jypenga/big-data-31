import os
import ray
import duckdb

import pandas as pd

@ray.remote
def column_cleaner(db, colname, cleaner=lambda x: x, tables=['train', 'test', 'validation']):
    dfs = []
    conn = duckdb.connect(db, read_only=True)

    for name in tables:
        df = conn.execute(f'SELECT tconst, {colname} FROM {name}').fetchdf()
        df[colname] = df[colname].apply(cleaner)
        dfs.append(df)

    conn.close()

    names = [f'{name}_{colname}' for name in tables]

    return names, dfs