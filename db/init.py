import os
import duckdb
import requests

import pandas as pd

from io import StringIO

NAME = 'unstructured.duckdb'

if __name__ == '__main__':
    # remove if already exists
    if os.path.exists(NAME):
        os.remove(NAME)

    # connect or create
    # conn = duckdb.connect(os.path.join(os.getcwd(), 'db', NAME), read_only=False)
    conn = duckdb.connect(os.path.join(NAME), read_only=False)
    
    # set path for data
    # data_root = os.path.join(os.getcwd(), 'imdb')
    data_root = os.path.join('..', 'dump')
    
    count = 0
    for fname in os.listdir(data_root):
        path = os.path.join(data_root, fname)
        
        if fname.startswith('train'):
            # load train csv in pandas df
            df = pd.read_csv(path)

            # register and create in duckdb
            new_name = ''.join(fname.split('.')[0].split('-'))
            conn.register(new_name, df)

            if count == 0:
                conn.execute(f'CREATE TABLE train AS SELECT * FROM {new_name}')
            else:
                conn.execute(f'INSERT INTO train SELECT * FROM {new_name}')

            count += 1

        # load test in duckdb
        elif fname.startswith('test'):
            df = pd.read_csv(path)

            conn.register('test', df)
            conn.execute('CREATE TABLE test AS SELECT * FROM test')

        # load validation in duckdb
        elif fname.startswith('validation'):
            df = pd.read_csv(path)

            conn.register('validation', df)
            conn.execute('CREATE TABLE validation AS SELECT * FROM validation')
        
        elif fname.endswith('.json'):
            # load json in pandas df
            df = pd.read_json(path)

            # register and create in duckdb
            new_name = fname.replace('.json', '')
            conn.register(new_name, df)

            conn.execute(f'CREATE TABLE {new_name} AS SELECT * FROM {new_name}')

    # get tweet genres
    # external = 'https://raw.githubusercontent.com/sidooms/MovieTweetings/master/latest/movies.dat'
    # response = requests.get(external)

    # df = pd.read_table(StringIO(response.content.decode('utf-8')), sep='::', engine='python', names=['tconst', 'title', 'genre'])

    # conn.register('mt_genres', df)
    # conn.execute('CREATE TABLE mt_genres AS SELECT * FROM mt_genres')

    # get tweet ratings
    external = 'https://raw.githubusercontent.com/sidooms/MovieTweetings/master/latest/ratings.dat'
    response = requests.get(external)

    df = pd.read_table(StringIO(response.content.decode('utf-8')), sep='::', engine='python', names=['tconst', 'rating', 'user_id'])

    conn.register('mt_ratings', df)
    conn.execute('CREATE TABLE mt_ratings AS SELECT * FROM mt_ratings')

    # get GroupLens/MovieLens links
    df = pd.read_csv(os.path.join('..', 'dump', 'links.csv'))

    conn.register('ml_links', df)
    conn.execute(f'CREATE TABLE ml_links AS SELECT * FROM ml_links')

    # get GroupLens/MovieLens ratings
    df = pd.read_csv(os.path.join('..', 'dump', 'ratings.csv'))

    conn.register('ml_ratings', df)
    conn.execute(f'CREATE TABLE ml_ratings AS SELECT * FROM ml_ratings')

    conn.close()
            
