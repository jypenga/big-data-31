import os
import ray
import duckdb

import pandas as pd

@ray.remote
def table_transformer(db, tabname, colname, sql, tables=['train', 'test', 'validation']):
    dfs = []
    conn = duckdb.connect(db, read_only=True)
    df = conn.execute(sql).fetchdf()

    df.tconst = df.tconst.apply(lambda x: f'tt{str(x).zfill(7)}')
    conn.register('temp', df)

    for name in tables:
        df = conn.execute(f'SELECT tconst FROM {name}').fetchdf()

        conn.register(f'{name}_ids', df)
        df = conn.execute(f"""SELECT {name}_ids.tconst, temp.{colname}
                          FROM temp 
                          INNER JOIN {name}_ids
                          ON temp.tconst = {name}_ids.tconst""").fetchdf()
        dfs.append(df)

    names = [f'{name}_{tabname}' for name in tables]

    return names, dfs