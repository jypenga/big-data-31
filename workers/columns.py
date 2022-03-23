import os
import duckdb

import pandas as pd


def column_cleaner(conn, colname, cleaner=lambda x: x, tables=['train', 'test', 'validation']):
    dfs = []

    for name in tables:
        df = conn.execute(f'SELECT tconst, {colname} FROM {name}').fetchdf()
        df[colname] = df[colname].apply(cleaner)
        dfs.append(df)

    conn.close()

    names = [f'{name}_{colname}' for name in tables]

    return names, dfs