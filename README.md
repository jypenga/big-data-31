Make sure that all imdb data and extra files are contained in /dump

cd db

python init.py

cd ..

python controller.py

cd ml

python ml/train.py

python ml/predict.py