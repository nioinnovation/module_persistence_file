import json
import os
import pickle


def load_json(path):
    data = {}
    if os.path.isfile(path):
        with open(path, 'r') as f:
            data = json.loads(f.read())
    return data


def save_json(path, data, **kwargs):
    with open(path, 'w+') as f:
        json.dump(data, f, **kwargs)


def load_pickle(path):
    data = {}
    if os.path.isfile(path):
        with open(path, 'rb') as f:
            data = pickle.load(f)
    return data


def save_pickle(path, data):
    with open(path, 'wb+') as f:
        pickle.dump(data, f)
