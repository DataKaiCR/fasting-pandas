from functools import wraps
from time import perf_counter
from typing import Tuple, Any
import pandas as pd


def calculate_memory_usage(df_input: pd.DataFrame) -> pd.Series:
    """Returns the real memory usage of a DataFrame including object types."""

    memory_usage = df_input.memory_usage(deep=True)
    print("--- Memory consumption ---")
    print(memory_usage, end="\n\n")

    return memory_usage


def calculate_percentage_difference(before: pd.Series, after: pd.Series) -> pd.Series:
    """Return the percentage difference of two pandas Series."""
    print("--- Memory consumption difference ---")
    return (after - before) / before * 100


def timeit(func: Any) -> Tuple[Any, float]:
    """A wrapper function to measure the execution time of a given function.
    Args:
        func: the function to be executed and timed
    Returns:
        a tuple containing the result of the function and the execution time in seconds
    """
    
    # Preserve the metadata of the original function with the 'wraps' decorator
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Record the start time using the perf_counter() function
        start_time = perf_counter()
        
        # Execute the function with the given arguments and store the result
        result = func(*args, **kwargs)
        
        # Record the end time and calculate the execution time in seconds
        end_time = perf_counter()
        execution_time = end_time - start_time
        
        # Print the function name and execution time to the console with 5 decimal places
        print(f"Function '{func.__name__}' took {execution_time:.5f} seconds to execute.")
        
        # Return the result and execution time as a tuple
        return result, execution_time
    
    # Return the wrapper function as a callable object
    return wrapper
