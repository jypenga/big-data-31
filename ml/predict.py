import os
import math
import time
import keras
import duckdb

import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler

from utils import build_query
from models import XGBClassifier


if __name__ == '__main__':
    conn = duckdb.connect(os.path.join('..', 'db', 'structured.duckdb'), read_only=True)

    test_merged_df = conn.execute(build_query(conn, 'test')).fetchdf()

    tconst_order = conn.execute('SELECT tconst FROM test_endYear').fetchdf()
    tconst_order['order'] = range(len(tconst_order))
    test_merged_df = test_merged_df.merge(tconst_order, on='tconst').sort_values(by=['order']).drop('order', axis=1)

    validation_merged_df = conn.execute(build_query(conn, 'validation')).fetchdf()

    tconst_order = conn.execute('SELECT tconst FROM validation_endYear').fetchdf()
    tconst_order['order'] = range(len(tconst_order))
    validation_merged_df = validation_merged_df.merge(tconst_order, on='tconst').sort_values(by=['order']).drop('order', axis=1)

    f = lambda x: 1 if x[0] != '' and x[1] != x[0] else 0
    test_merged_df['renamed'] = test_merged_df[['originalTitle', 'primaryTitle']].apply(f, axis=1)
    validation_merged_df['renamed'] = validation_merged_df[['originalTitle', 'primaryTitle']].apply(f, axis=1)

    f = lambda x: x[0] if math.isnan(x[1]) else x[1]
    test_merged_df['mtRatings'] = test_merged_df[['mtRatings', 'mlRatings']].apply(f, axis=1)
    test_merged_df = test_merged_df.drop('mlRatings', axis=1)

    validation_merged_df['mtRatings'] = validation_merged_df[['mtRatings', 'mlRatings']].apply(f, axis=1)
    validation_merged_df = validation_merged_df.drop('mlRatings', axis=1)

    test_merged_df = test_merged_df.drop(['originalTitle', 'primaryTitle', 'tconst'], axis=1)
    validation_merged_df = validation_merged_df.drop(['originalTitle', 'primaryTitle', 'tconst'], axis=1)

    test_with_rating_index = np.where(test_merged_df['mtRatings'].notnull())[0]
    test_without_rating_index = np.where(test_merged_df['mtRatings'].isnull())[0]

    validation_with_rating_index = np.where(validation_merged_df['mtRatings'].notnull())[0]
    validation_without_rating_index = np.where(validation_merged_df['mtRatings'].isnull())[0]

    print(f"{len(test_with_rating_index)} + {len(test_without_rating_index)} = {len(test_merged_df)}")
    print(f"{len(validation_with_rating_index)} + {len(validation_without_rating_index)} = {len(validation_merged_df)}")

    test_with_rating = test_merged_df.iloc[test_with_rating_index]
    test_without_rating = test_merged_df.iloc[test_without_rating_index]
    test_without_rating = test_without_rating.drop('mtRatings', axis=1)

    validation_with_rating = validation_merged_df.iloc[validation_with_rating_index]
    validation_without_rating = validation_merged_df.iloc[validation_without_rating_index]
    validation_without_rating = validation_without_rating.drop('mtRatings', axis=1)

    full_test_array_without_ratings = test_without_rating.to_numpy()
    full_test_array_with_ratings = test_with_rating.to_numpy()

    full_validation_array_without_ratings = validation_without_rating.to_numpy()
    full_validation_array_with_ratings = validation_with_rating.to_numpy()

    standardizer = StandardScaler()
    full_test_array_without_ratings = standardizer.fit_transform(full_test_array_without_ratings)
    full_test_array_with_ratings = standardizer.fit_transform(full_test_array_with_ratings)
    full_validation_array_without_ratings = standardizer.fit_transform(full_validation_array_without_ratings)
    full_validation_array_with_ratings = standardizer.fit_transform(full_validation_array_with_ratings)

    # model_without_ratings = XGBClassifier()
    # model_without_ratings.load_model('xgb_without_ratings.json')
    # model_with_ratings = XGBClassifier()
    # model_with_ratings.load_model('xgb_with_ratings.json')

    model_without_ratings = keras.models.load_model('keras2model_without_ratings')
    model_with_rating = keras.models.load_model('keras2model_without_ratings')
    
    test_predictions_without_ratings = model_without_ratings.predict(full_test_array_without_ratings)
    test_predictions_without_ratings = list(map(lambda x: False if x<0.5 else True, test_predictions_without_ratings))

    test_predictions_with_ratings = model_with_ratings.predict(full_test_array_with_ratings)
    test_predictions_with_ratings = list(map(lambda x: False if x<0.5 else True, test_predictions_with_ratings))

    validation_predictions_without_ratings = model_without_ratings.predict(full_validation_array_without_ratings)
    validation_predictions_without_ratings = list(map(lambda x: False if x<0.5 else True, validation_predictions_without_ratings))

    validation_predictions_with_ratings = model_with_ratings.predict(full_validation_array_with_ratings)
    validation_predictions_with_ratings = list(map(lambda x: False if x<0.5 else True, validation_predictions_with_ratings))

    test_predictions_without_ratings_df = pd.DataFrame(test_predictions_without_ratings, index=test_without_rating_index)
    test_predictions_with_ratings_df = pd.DataFrame(test_predictions_with_ratings, index=test_with_rating_index)

    validation_predictions_without_ratings_df = pd.DataFrame(validation_predictions_without_ratings, index=validation_without_rating_index)
    validation_predictions_with_ratings_df = pd.DataFrame(validation_predictions_with_ratings, index=validation_with_rating_index)

    final_test_predictions = pd.concat([test_predictions_without_ratings_df, test_predictions_with_ratings_df], axis=0).sort_index()[0]
    final_validation_predictions = pd.concat([validation_predictions_without_ratings_df, validation_predictions_with_ratings_df], axis=0).sort_index()[0]

    validation_merged_df['label'] = final_validation_predictions
    print(validation_merged_df['label'].value_counts())

    test_merged_df['label'] = final_test_predictions
    print(test_merged_df['label'].value_counts())

    final_test_predictions.to_csv(os.path.join('predictions', f'test_predictions.txt'), header=False, index=False)
    final_validation_predictions.to_csv(os.path.join('predictions', f'validation_predictions.txt'), header=False, index=False)
