import os
import ray
import duckdb
import configparser

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
def column_cleaner_R(fname):
    config = configparser.ConfigParser()
    config.readfp(open(os.path.join('R', 'config.txt')))
    r_path = os.path.join(*config.get('Paths', 'r_path').split('\\'), 'bin', 'Rscript.exe')

    outp = os.popen(f'{r_path} {os.path.join("R", fname)}')
    outp.read()
    outp.close()
    
    return