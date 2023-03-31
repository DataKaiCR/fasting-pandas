from typing import List, Optional, IO
from pathlib import Path
import seaborn as sns
import matplotlib.pyplot as plt
import os
import pandas as pd
import fasting_pandas as fp

PROJECT_DIR = Path().absolute()
DATA_DIR = os.path.join(PROJECT_DIR, 'data')


def set_benchmarks(df_sizes: List[int], file_formats: List[str], db: Optional[str] = None) -> pd.DataFrame:
    # If db is not None then save results to a local database.
    if db:
        db = db

    # Create an empty DataFrame to store the results
    results_df = pd.DataFrame()

    for size in df_sizes:
        raw_df = fp.datasets.generate_teamresult_df(size)
        optimized_df = fp.datasets.set_dtypes_for_teamresult_df(raw_df)

        dfs = {False: raw_df, True: optimized_df}
        for key, df in dfs.items():
            tdf = fp.TimedPandas(df)

            for file_format in file_formats:
                if file_format == 'parquet':
                    tdf.prob = tdf.prob.astype('float32')
                file_name = f"dataset_{size}.{file_format}"
                file_path = os.path.join(DATA_DIR, file_name)
                # Write
                _, result = tdf.time(
                    f'to_{file_format}', file_path, db_path=db)
                results_df = pd.concat([results_df, pd.DataFrame({
                    'method_name': [result.method_name],
                    'file_format': [file_format],
                    'dataframe_size': [result.shape[0]],
                    'is_datatype_optimized': [key],
                    'time_seconds': [result.elapsed_time],
                    'memory_usage_mb': [result.memory_usage / 1000],
                    'file_size_mb': [result.file_size / 1000]
                })],
                    ignore_index=True)
                # Read
                _, result = tdf.time(
                    f'read_{file_format}', file_path, db_path=db,  is_read_method=True)
                results_df = pd.concat([results_df, pd.DataFrame({
                    'method_name': [result.method_name],
                    'file_format': [file_format],
                    'dataframe_size': [result.shape[0]],
                    'is_datatype_optimized': [key],
                    'time_seconds': [result.elapsed_time],
                    'memory_usage_mb': [result.memory_usage / 1000],
                    'file_size_mb': [result.file_size / 1000]
                })],
                    ignore_index=True)

    return results_df


def create_plots(timed_results: pd.DataFrame, save: Optional[bool] = False, save_path: Optional[str] = None) -> Optional[IO]:
    """
    Create various plots and correlations based on the timed results.

    Args:
        timed_results (pd.DataFrame): A DataFrame containing timed results for different methods on different dataframes.
        save (bool): Will save results in png format instead of showing them.
        save_path (str): Directory to save the files. If none will default to data/.
    """
    if save and not save_path:
        save_path = DATA_DIR
    # Create a lineplot of time vs file type read and write
    sns.set_style("whitegrid")
    sns.set(rc={'figure.figsize': (12, 8)})
    sns.barplot(x="method_name", y="time_seconds", data=timed_results)
    plt.title('Benchmark Results')
    plt.xlabel('Method')
    plt.ylabel('Time (seconds)')
    if save:
        plt.savefig(os.path.join(save_path, 'time_vs_filetype.png'))
    else:
        plt.show()

    # Create scatterplot of time vs memory usage
    sns.scatterplot(x='time_seconds', y='memory_usage_mb',
                    hue='method_name', data=timed_results)
    plt.xlabel('Time (seconds)')
    plt.ylabel('Memory Usage (MB)')
    plt.title('Time vs Memory Usage')
    if save:
        plt.savefig(os.path.join(save_path, 'time_vs_memory_usage.png'))
    else:
        plt.show()

    # Create scatterplot of file_size_mb vs time
    sns.scatterplot(x='file_size_mb', y='time_seconds',
                    hue='method_name', data=timed_results)
    plt.xlabel('file_size_mb (MB)')
    plt.ylabel('Time (seconds)')
    plt.title('file_size_mb vs Time')
    if save:
        plt.savefig(os.path.join(save_path, 'file_size_mb_vs_time.png'))
    else:
        plt.show()

    # Create heatmap of correlation between variables
    corr = timed_results[['time_seconds',
                          'memory_usage_mb', 'file_size_mb']].corr()
    sns.heatmap(corr, annot=True)
    plt.title('Correlation Between Variables')
    if save:
        plt.savefig(os.path.join(
            save_path, 'time_vs_memory_usage_vs_file_size.png'))
    else:
        plt.show()

    # Create scatterplot of time vs memory usage, grouped by size of the dataframe
    grouped_results = timed_results.groupby('dataframe_size').agg(
        time_seconds=pd.NamedAgg(column='time_seconds', aggfunc='mean'),
        memory_usage_mb=pd.NamedAgg(column='memory_usage_mb', aggfunc='mean')
    ).reset_index()

    sns.scatterplot(x='time_seconds', y='memory_usage_mb',
                    hue='dataframe_size', data=grouped_results)
    plt.xlabel('Time (seconds)')
    plt.ylabel('Memory Usage (MB)')
    plt.title('Time vs Memory Usage, Grouped by Size of DataFrame')
    if save:
        plt.savefig(os.path.join(
            save_path, 'time_vs_memory_usage_grouped_by_dataframe_size.png'))
    else:
        plt.show()

    # Create scatterplot of file_size_mb vs time, grouped by size of the dataframe
    sns.scatterplot(x='file_size_mb', y='time_seconds',
                    hue='dataframe_size', data=timed_results)
    plt.xlabel('file_size_mb (MB)')
    plt.ylabel('time_seconds')
    plt.title('file_size_mb vs Time, Grouped by Size of DataFrame')
    if save:
        plt.savefig(os.path.join(
            save_path, 'time_vs_file_size_grouped_by_dataframe_size.png'))
    else:
        plt.show()


def cleanup(dir_path: str, *file_formats: str, all: bool = False) -> None:
    """
    Remove all files with specified file formats from the given directory path.

    Args:
        dir_path (str): path to directory to remove files from
        *file_formats (str or list of str): file format(s) to remove from directory. If all=True, this parameter is ignored.
        all (bool): if True, remove all files from directory regardless of file format(s) specified in *file_formats parameter.

    Returns:
        None
    """
    if all:
        file_formats = []
    else:
        # Flatten the file_formats argument if it is a list
        if len(file_formats) == 1 and isinstance(file_formats[0], list):
            file_formats = file_formats[0]

    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        # If all=True, remove all files. Otherwise, only remove files with specified file formats.
        if all or any(filename.endswith('.' + ext) for ext in file_formats):
            try:
                os.remove(file_path)
            except FileNotFoundError as e:
                print(e)
                continue


def main():
    # For simplicity we will nuke the database to start from scratch.
    cleanup(DATA_DIR, ['db'])
    # We will generate benchmark data and save it to the database.
    benchmarks = set_benchmarks([1_000, 100_000, 1_000_000, 10_000_000], [
                                'csv', 'json', 'pickle', 'parquet'], db= os.path.join(DATA_DIR, 'benchmarks.db'))
    # Generate some simple graphs. I honestly just did some random plots without purpose.
    create_plots(benchmarks, save=True)
    # Remove junk testing files.
    cleanup(DATA_DIR, ['csv', 'json', 'pickle', 'parquet'])


if __name__ == '__main__':
    main()
