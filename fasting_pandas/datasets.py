import pandas as pd
import numpy as np


def generate_scores(size=10_000):
    df = pd.DataFrame()
    size = 10_000
    df['age'] = np.random.randint(12,20,size)
    df['study_time'] = np.random.randint(0,11,size)
    df['test_1_score'] = np.random.rand(size) 
    df['test_2_score'] = np.random.rand(size) 
    df['test_3_score'] = np.random.rand(size) 
    df['happy_food'] = np.random.choice(['pizza', 'hamburguer', 'fried-chicken','nachos', 'grilled-meat'], size)
    df['sad_food'] = np.random.choice(['beef-liver', 'tomatoes','broccoli','soup', 'beans'], size)
    return df

def generate_results(size=10_000):
    df = pd.DataFrame()
    df['size'] = np.random.choice(['big', 'medium', 'small'], size)
    df['age'] = np.random.randint(1,50,size)
    df['team'] = np.random.choice(['yellow', 'cyan', 'magenta', 'violet', 'black'], size)
    df['result'] = np.random.choice(['win', 'lose'], size)
    dates = pd.date_range('2015-01-01', '2020-12-31')
    df['date'] = np.random.choice(dates, size)
    df['prob'] = np.random.uniform(0,1,size)
    return df

def set_dtypes_for_results(df):
    df['size'] = df['size'].astype('category')
    df['age'] = df['age'].astype('int8')
    df['team'] = df['team'].astype('category')
    df['win'] = df['result'].map({'win': True, 'lose': False})
    df['prob'] = df['prob'].astype('float16')
    return df