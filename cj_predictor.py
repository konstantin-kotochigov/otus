import pandas
import numpy
import pickle
import json
import random
import os

from sklearn.model_selection import train_test_split, StratifiedShuffleSplit
from sklearn.metrics import roc_auc_score

import keras.preprocessing
from keras.layers import Concatenate
from keras.layers import MaxPooling1D
from keras.layers import Embedding, LSTM, Dense, PReLU, Dropout, BatchNormalization
from keras.layers import Conv1D
from keras.preprocessing import sequence
from keras.layers import Input, Dense, Reshape, Flatten, Dropout, multiply, GaussianNoise
from keras.layers import BatchNormalization, Activation, Embedding, ZeroPadding2D
from keras.models import Sequential, Model, load_model
import keras.backend as K

class CJ_Predictor:
    
    input_data = None
    model_path = None
    return_model = None
    
    model_path = None
    vocabulary_path = None
    model_weights_path = None
    
    train_auc = test_auc = test_auc_std = None
    
    def __init__(self, model_path, hdfs_client):
        print("Created Predictor Instance With Working Dir = {}".format(model_path))
        self.model_path = model_path
        self.vocabulary_path = model_path + "/vocabulary.pkl"
        self.model_weights_path = model_path + "/model.pkl"
        self.hdfs_client = hdfs_client
    
    def set_data(self, input_data):
        self.input_data = input_data
    
    def preprocess_data(self, model_update):
    
        data = self.input_data
        
        # Preprocess "TS Deltas" Sequence
        data['dt'] = data.dt.apply(lambda x: list(x)[0:len(x)-1])
        data.loc[:, 'dt'] = data.dt.apply(lambda r: [0]*(32-len(r)) + r if pandas.notna(numpy.array(r).any()) else [0]*32 )
        data.dt = data.dt.apply(lambda r: numpy.log(numpy.array(r)+1))
        Max = numpy.max(data.dt.apply(lambda r: numpy.max(r)))
        data.dt = data.dt.apply(lambda r: r / Max)
        data.dt = data.dt.apply(lambda r: r if len(r)==32 else r[-32:])
        
        # Preprocess Urls Sequence
        if model_update==True:
            tk = keras.preprocessing.text.Tokenizer(filters='', split=' ')
            tk.fit_on_texts(data.url.values)
            self.hdfs_client.write(self.vocabulary_path, data=pickle.dumps(tk), overwrite=True)
        else:
            with self.hdfs_client.read(self.vocabulary_path) as reader:
                tk = pickle.loads(reader.read())
            
        urls = tk.texts_to_sequences(data.url)
        urls = sequence.pad_sequences(urls, maxlen=32)
        
        dt = numpy.concatenate(data.dt.values).reshape((len(data), 32, 1))
        return (urls, dt, data['target'], tk)
    
    def create_network(self, tk):
    
        input_layer1 = Input(shape=(32,))
        layer11 = Embedding(len(tk.word_index)+1, 4, input_length=32, trainable=True)(input_layer1)
        layer12 = LSTM(16, dropout_W=0.2, dropout_U=0.2, return_sequences=False)(layer11)
        
        input_layer2 = Input(shape=(32,1))
        layer21 = LSTM(16, dropout_W=0.2, dropout_U=0.2, return_sequences=False)(input_layer2)
        
        layer1 = Concatenate(axis=-1)([layer12, layer21])
        
        layer2 = Dense(16)(layer1)
        layer3 = Dense(4)(layer2)
        layer4 = Dense(1)(layer3)
        
        output_layer = Activation('sigmoid')(layer4)
        return_model = Model(inputs=[input_layer1, input_layer2], outputs=output_layer)
        
        return_model.compile(loss='binary_crossentropy',
                    optimizer='Adam',
                    metrics=['accuracy'])
        
        return return_model
    
    # def main():
    #     import load
    #    import functions
    #    import cj_lstm
    #    import lstm
    #    import export
    #    return -1
    
    # data = pandas.read_parquet("/home/kkotochigov/bmw_cj_lstm.parquet")
    # data1 = pandas.read_parquet("/home/kkotochigov/bmw_cj_lstm1.parquet")
    
    def optimize(self, batch_size):
        
        auc_mean = []
        auc_std = []
        urls, dt, y, tk = self.preprocess_data(model_update=True)
        
        models_to_test = [self.create_network(tk)]
        # models_to_test = [self.create_network(tk)]
        
        for model_num, model in enumerate(models_to_test):
        
            cv_number = 0
            auc = []
            
            for cv_train_index, cv_test_index in StratifiedShuffleSplit(n_splits=15, train_size=0.5, test_size=0.15, random_state=123).split(y,y):
                
                cv_number += 1
                print("Fitting Model (CV={}) train length = {}, test length = {}".format(cv_number, len(cv_train_index), len(cv_test_index)))
                
                train = ([urls[cv_train_index], dt[cv_train_index]], y[cv_train_index])
                test = ([urls[cv_test_index], dt[cv_test_index]], y[cv_test_index])
                model.fit(train[0], train[1], epochs=1, batch_size=batch_size, shuffle = True)
                
                current_auc = roc_auc_score(test[1], model.predict(test[0]))
                auc.append(current_auc)
                
            print("Model = {}, average AUC = {:06.5f}, std AUC = {:06.5f}, [{:06.5f},{:06.5f}]".format(model_num, numpy.mean(auc), numpy.std(auc), numpy.min(auc), numpy.max(auc)))
            auc_mean.append(numpy.mean(auc))
            auc_std.append(numpy.std(auc))
            
        return (auc_mean, auc_std)
    
    def fit(self, update_model, batch_size):
        
        urls, dt, y, tk = self.preprocess_data(update_model)
        model = self.create_network(tk)
        
        train_data = ([urls, dt], y)
        scoring_data = [urls[self.input_data.target==0], dt[self.input_data.target==0]]
        
        if update_model:
            model.fit(train_data[0], train_data[1], epochs=1, batch_size=batch_size, shuffle = True)
            self.hdfs_client.write(self.model_weights_path, pickle.dumps(model), overwrite=True)
        else:
            with self.hdfs_client.read(self.model_weights_path) as reader:
                model = pickle.loads(reader.read())
        
        print("Scoring Data...")
        pred = model.predict(scoring_data)
        self.result = pandas.DataFrame({"fpc":self.input_data.fpc[self.input_data.target==0], "tpc":self.input_data.tpc[self.input_data.target==0], "return_score":pred.reshape(-1)})
        self.train_auc = round(roc_auc_score(train_data[1], model.predict(train_data[0], batch_size=batch_size)), 2)
        
        self.result['return_score'] = pandas.cut(self.result.return_score, 5, labels=['1','2','3','4','5'])
        
        
        
        return self.result