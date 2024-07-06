import os
import json


def load_config(config_path=None):
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), '', 'config.json')

    if os.path.isfile(config_path):
        with open(config_path) as data_file:
            data = json.load(data_file)
            return data
