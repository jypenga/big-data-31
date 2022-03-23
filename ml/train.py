import os
import math
import keras
import duckdb
import keras.optimizers

import tensorflow as tf

from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

from models import MLP, XGBClassifier
from utils import build_query

EPOCHS = 10

if __name__ == '__main__':
    conn = duckdb.connect(os.path.join('..', 'db', 'structured.duckdb'), read_only=True)

    merged_df = conn.execute(build_query(conn, 'train')).fetchdf()
    
    tconst_order = conn.execute('SELECT tconst FROM train_endYear').fetchdf()
    tconst_order['order'] = range(len(tconst_order))
    merged_df = merged_df.merge(tconst_order, on='tconst').sort_values(by=['order']).drop('order', axis=1)

    f = lambda x: 1 if x[0] != '' and x[1] != x[0] else 0
    merged_df['renamed'] = merged_df[['originalTitle', 'primaryTitle']].apply(f, axis=1)

    f = lambda x: x[0] if math.isnan(x[1]) else x[1]
    merged_df['mtRatings'] = merged_df[['mtRatings', 'mlRatings']].apply(f, axis=1)
    merged_df = merged_df.drop('mlRatings', axis=1)

    merged_df = merged_df.drop(['originalTitle', 'primaryTitle', 'tconst'], axis=1)

    merged_df_without_ratings = merged_df.drop('mtRatings', axis=1)
    merged_df_with_ratings = merged_df.dropna()

    y_without_ratings = merged_df_without_ratings['label']
    y_without_ratings = y_without_ratings.to_numpy(dtype=int)
    merged_df_without_ratings = merged_df_without_ratings.drop('label', axis=1)

    y_with_ratings = merged_df_with_ratings['label']
    y_with_ratings = y_with_ratings.to_numpy(dtype=int)
    merged_df_with_ratings = merged_df_with_ratings.drop('label', axis=1)

    X_without_ratings = merged_df_without_ratings.to_numpy()
    X_with_ratings = merged_df_with_ratings.to_numpy()

    standardizer = StandardScaler()
    X_without_ratings = standardizer.fit_transform(X_without_ratings)
    X_with_ratings = standardizer.fit_transform(X_with_ratings)

    X_train_without_ratings, X_test_without_ratings, y_train_without_ratings, y_test_without_ratings = \
    train_test_split(X_without_ratings, y_without_ratings, test_size=0.2, random_state=1)

    X_train_with_ratings, X_test_with_ratings, y_train_with_ratings, y_test_with_ratings = \
    train_test_split(X_with_ratings, y_with_ratings, test_size=0.2, random_state=1)

    # keras2model_without_ratings = MLP(input_shape=(5,))
    # keras2model_without_ratings.compile(optimizer='adam',
    #                                     loss='binary_crossentropy',
    #                                     metrics=['accuracy'])

    # print("===== Training without ratings")
    # keras2model_without_ratings.fit(X_train_without_ratings, y_train_without_ratings, epochs=EPOCHS, batch_size=1)
    # keras2model_without_ratings.save('keras2model_without_ratings')

    # loss_and_metrics = keras2model_without_ratings.evaluate(X_test_without_ratings, y_test_without_ratings)
    # print('Loss without ratings = ', loss_and_metrics[0])
    # print('Accuracy without ratings = ', loss_and_metrics[1])

    # keras2model_with_ratings = MLP(input_shape=(6,))
    # keras2model_with_ratings.compile(optimizer='adam',
    #                                  loss='binary_crossentropy',
    #                                  metrics=['accuracy'])

    # print("===== Training with ratings")
    # keras2model_with_ratings.fit(X_train_with_ratings, y_train_with_ratings, epochs=EPOCHS, batch_size=1)
    # keras2model_with_ratings.save('keras2model_with_ratings')

    # loss_and_metrics = keras2model_with_ratings.evaluate(X_test_with_ratings, y_test_with_ratings)
    # print('Loss with ratings = ', loss_and_metrics[0])
    # print('Accuracy with ratings = ', loss_and_metrics[1])

    model = XGBClassifier()
    model.fit(X_train_without_ratings, y_train_without_ratings)
    model.save_model('xgb_without_ratings.json')

    print(model.score(X_test_without_ratings, y_test_without_ratings))

    model = XGBClassifier()
    model.fit(X_train_with_ratings, y_train_with_ratings)
    model.save_model('xgb_with_ratings.json')

    print(model.score(X_test_with_ratings, y_test_with_ratings))
