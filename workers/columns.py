import os
import ray
import math
import duckdb
import configparser

import numpy as np
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


@ray.remote
def column_cleaner_avg(db, colname, tables=['train', 'test', 'validation']):
    dfs = []
    conn = duckdb.connect(db, read_only=True)

    for name in tables:
        df = conn.execute(f'SELECT tconst, {colname} FROM {name}').fetchdf()
        df_copy = df[colname].replace(r'\N', np.nan)
        avg = df_copy.dropna().to_numpy(dtype=int).mean()

        df[colname] = df[colname].apply(lambda x: avg if x == r'\N' else int(x))
        dfs.append(df)

    conn.close()

    names = [f'{name}_{colname}' for name in tables]

    return names, dfs


@ray.remote
def column_cleaner_nan_avg(db, colname, tables=['train', 'test', 'validation']):
    dfs = []
    conn = duckdb.connect(db, read_only=True)

    def f2(x, fill=0):
        if x == 'nan' or x == "NaN" or math.isnan(x):
            return fill
        else:
            return int(x)

    for name in tables:
        df = conn.execute(f'SELECT tconst, {colname} FROM {name}').fetchdf()
        df_copy = df[colname]
        avg = df_copy.dropna().to_numpy(dtype=int).mean()

        df[colname] = df[colname].apply(lambda x: f2(x, avg))
        dfs.append(df)

    conn.close()

    names = [f'{name}_{colname}' for name in tables]

    return names, dfs


@ray.remote
def column_cleaner_R(fname):
    config = configparser.ConfigParser()
    config.readfp(open(os.path.join('R', 'config.txt')))
    r_path = os.path.join(*config.get('Paths', 'r_path').split('\\'), 'bin', 'Rscript.exe')

    outp = os.popen(f'{r_path} {os.path.join("R", fname)}')
    outp.read()
    outp.close()
    
    return