from pathlib import Path
import os
import sys
import importlib

''' This is a simple config library for easier interaction with the jupyter notebooks.
    I have mapped the main project directory and data directory to avoid code duplication.
    I also appended the PARENT_DIR to sys, in order to import the fasting pandas library for the notebook.
    I am aware of the faulty practice. The only thing I can say is that I'm sorry for bringing this havoc into light.
'''
PARENT_DIR = os.path.abspath(os.path.join(Path().absolute(),os.pardir))
DATA_DIR = os.path.join(PARENT_DIR, 'data')

sys.path.append(PARENT_DIR)
