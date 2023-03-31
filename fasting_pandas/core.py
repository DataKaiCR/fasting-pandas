import pandas as pd
import sqlite3
import os
from time import perf_counter
from typing import Tuple, Optional, List, Union


class TimedContext:
    """
    Context manager that times how long a code block takes to execute.
    """

    def __init__(self, context_manager):
        self.context_manager = context_manager

    def __enter__(self):
        self.start_time = perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed_time = perf_counter() - self.start_time


class TimedPandas(pd.DataFrame):
    """
    Subclass of pandas.DataFrame that times how long a method takes to execute.
    """
    def time(self, method_name: str, *args, db_path: str = None,
             truncate_table: bool = False, is_read_method: bool = False,
             **kwargs) -> Tuple:
        """
        Times how long a method takes to execute, and returns the result along with timing information.

        Args:
            method_name (str): The name of the method to time.
            *args: Positional arguments to pass to the method.
            db_path (str, optional): Path to a SQLite database where the timing information should be saved.
            truncate_table (bool, optional): Deletes everything from result table before inserting new data.
            is_read_method (bool, optional): Uses the pd library instead of the dataframe to measure reading time.
            **kwargs: Keyword arguments to pass to the method.

        Returns:
            tuple: A tuple containing the result of the method and a TimedResult object with timing information.
        """
        file_size = None
        if is_read_method:
            # df = pd.DataFrame()
            file_path = args[0]
            file_size = os.path.getsize(file_path)
            with TimedContext(self) as c:
                method = getattr(pd, method_name)
                result = method(*args, **kwargs)
            memory_usage = result.memory_usage(deep=True).sum()
            memory_usage_detail = result.memory_usage(deep=True)
            # print(result)
        else:
            with TimedContext(self) as c:
                method = getattr(super(), method_name)
                result = method(*args, **kwargs)
            if method_name in ['to_csv', 'to_excel', 'to_json', 'to_pickle', 'to_parquet']:
                file_path = args[0]
                file_size = os.path.getsize(file_path)
                # print(file_size)
            memory_usage = self.memory_usage(deep=True).sum()
            memory_usage_detail = self.memory_usage(deep=True)
        timed_result = TimedResult(
            method_name,
            self.__class__.__name__,
            self.shape,
            memory_usage,
            memory_usage_detail,
            c.elapsed_time,
            file_size
        )

        if db_path:
            timed_result.save_to_db(db_path, truncate_table=truncate_table)

        return result, timed_result


class TimedResult:
    """
    Stores timing information about a method call.
    """

    def __init__(self, method_name: str, class_name, shape, memory_usage, memory_usage_detail, elapsed_time, file_size):
        """
        Initializes a new TimedResult object with the specified timing information.

        Args:
            method_name (str): The name of the method that was timed.
            class_name (str): The name of the class that the method belongs to.
            shape (tuple): The shape of the DataFrame that the method was called on.
            memory_usage (int): The total memory usage of the DataFrame after the method was called.
            memory_usage_detail (str): The detailed memory usage report of the DataFrame.
            elapsed_time (float): The time in seconds that it took for the method to execute.
            file_size (float): The size of the file. Only applies for reading or writing methods.
            
        """
        self.method_name = method_name
        self.class_name = class_name
        self.shape = shape
        self.memory_usage = memory_usage
        self.memory_usage_detail = memory_usage_detail
        self.elapsed_time = elapsed_time
        self.file_size = file_size

    def __repr__(self):
        """
        Returns a string representation of the timing information.

        Returns:
            str: A string with the timing information formatted nicely.
        """
        return f"""Method: {self.method_name}\nClass: {self.class_name}\nShape: {self.shape}\nMemory Usage: {self.memory_usage}
        \nMemory Usage Detail: {self.memory_usage_detail} \nElapsed Time: {self.elapsed_time:.5f} seconds
        \nFile Size: {self.file_size}
        """

    def save_to_db(self, db_path: str, truncate_table: bool = False):
        """
        Saves the timing information to a SQLite database.

        Args:
            db_path (str): Path to the SQLite database where the information should be saved.
        """
        with sqlite3.connect(db_path) as conn:
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS results (
                    method_name TEXT,
                    class_name TEXT,
                    shape TEXT,
                    memory_usage FLOAT,
                    memory_usage_detail TEXT,
                    elapsed_time REAL,
                    file_size INT
                )
            """)
            shape_str = str(self.shape).replace("'", '"')
            memory_usage_str = self.memory_usage_detail.to_json()
            if truncate_table:
                c.execute("DELETE FROM results")
            c.execute('INSERT INTO results VALUES (?,?,?,?,?,?,?)',
                      (self.method_name,
                       self.class_name,
                       shape_str,
                       float(self.memory_usage),
                       memory_usage_str,
                       self.elapsed_time,
                       self.file_size)
                      )
            conn.commit()

    @classmethod
    def load_from_db(cls, db_path: str, method_name: Optional[str] = None) -> Union[List, Optional[Tuple]]:
        with sqlite3.connect(db_path) as conn:
            c = conn.cursor()
            if method_name:
                c.execute('SELECT * FROM results WHERE method_name = ?', (method_name,))
                result = c.fetchone()
                if result:
                    method_name, class_name, shape_str, memory_usage, memory_usage_str, elapsed_time, file_size = result
                    shape = tuple(map(int, shape_str.strip('()').split(',')))
                    memory_usage_detail = pd.read_json(memory_usage_str, typ='series')
                    return (cls(method_name, class_name, shape, memory_usage, memory_usage_detail, elapsed_time, file_size))
                else:
                    return None
            else:
                c.execute('SELECT * FROM results')
                results = c.fetchall()
                return tuple(cls(*result) for result in results)

    #     Other possible methods of interest.
    #                                , self.__class__.__name__
    #                                , self.shape
    #                             #    , self.index.nlevels
    #                             #    , self.columns.nlevels
    #                             #    , len(self), kwargs
    #                             #    , args, self.dtypes
    #                                , self.memory_usage(deep=True)
    #                             #    , self.index.dtype
    #                             #    , self.columns.dtype
    #                             #    , self.index.nunique()
    #                             #    , self.columns.nunique()
    #                             #    , self.index.memory_usage(deep=True)
    #                                , self.columns.memory_usage(deep=True)
    #                             #    , self.index.is_unique
    #                             #    , self.columns.is_unique
    #                             #    , self.index.has_duplicates
    #                             #    , self.columns.has_duplicates
    #                                , self.elapsed_time)
