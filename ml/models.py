import keras
import xgboost

import tensorflow as tf


def MLP(input_shape=5):
    return keras.Sequential([
    keras.layers.Flatten(input_shape=input_shape),
    keras.layers.Dense(16, activation=tf.nn.relu),
    keras.layers.Dense(32, activation=tf.nn.relu),
    keras.layers.Dense(16, activation=tf.nn.relu),
    keras.layers.Dense(1, activation=tf.nn.sigmoid),
    ])


def XGBClassifier():
    return xgboost.XGBClassifier()