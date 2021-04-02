import json
import os
from os.path import dirname


def get_text(json_file_name):
    with open(os.path.join(dirname(__file__), "resources", json_file_name)) as data_file:
        return data_file.read()


def get_json(json_file_name):
    with open(os.path.join(dirname(__file__), "resources", json_file_name)) as data_file:
        return json.load(data_file)