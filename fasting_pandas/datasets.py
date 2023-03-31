import pandas as pd
import numpy as np


def generate_testscore_df(size: int = 10_000) -> pd.DataFrame:
    """Returns a DataFrame with calculated test scores."""

    df = pd.DataFrame()
    size = 10_000
    df['age'] = np.random.randint(12, 20, size)
    df['study_time'] = np.random.randint(0, 11, size)
    df['test_1_score'] = np.random.rand(size)
    df['test_2_score'] = np.random.rand(size)
    df['test_3_score'] = np.random.rand(size)
    df['happy_food'] = np.random.choice(
        ['pizza', 'hamburguer', 'fried-chicken', 'nachos', 'grilled-meat'],
        size)
    df['sad_food'] = np.random.choice(
        ['beef-liver', 'tomatoes', 'broccoli', 'soup', 'beans'], size)

    return df


def generate_teamresult_df(size: int = 10_000) -> pd.DataFrame:
    """Returns a DataFrame with calculated game results."""

    df = pd.DataFrame()
    df['size'] = np.random.choice(['big', 'medium', 'small'], size)
    df['age'] = np.random.randint(1, 50, size)
    df['team'] = np.random.choice(
        ['yellow', 'cyan', 'magenta', 'violet', 'black'], size)
    dates = pd.date_range('2015-01-01', '2020-12-31')
    df['date'] = np.random.choice(dates, size)
    df['prob'] = np.random.uniform(0, 1, size)
    df['result'] = np.random.choice(['win', 'lose'], size)

    return df


def set_dtypes_for_teamresult_df(df_input: pd.DataFrame, inplace: bool = False) -> pd.DataFrame:
    """Optimizes the datatypes for the team result DataFrame."""
    # Create a copy of the DataFrame to avoid affecting the original
    if not inplace:
        df_output = df_input.copy()
    else:
        df_output = df_input
    df_output['size'] = df_output['size'].astype('category')
    df_output['age'] = df_output['age'].astype('int8')
    df_output['team'] = df_output['team'].astype('category')
    df_output['win'] = df_output['result'].map({'win': True, 'lose': False})
    df_output['prob'] = df_output['prob'].astype('float16')
    df_output.drop(columns='result', inplace=True)

    return df_output
