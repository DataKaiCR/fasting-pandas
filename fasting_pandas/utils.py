from functools import wraps
import time
def time_it(original_function):

    @wraps(original_function)
    def wrapper(*args, **kwargs):
        t1 = time.time()
        result = original_function(*args, **kwargs)
        t2 = time.time() - t1
        print('{} ran in {} sec'.format(original_function.__name__, t2))
        return result

    return wrapper