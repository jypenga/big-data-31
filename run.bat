pip install -r requirements.txt
cd db
python init.py
cd ..
python controller.py
cd ml
python train.py
python predict.py
cd ..