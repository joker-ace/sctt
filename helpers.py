import sys
import time
import threading
import pprint

from constants import MB


def get_obj_size_in_mb(object):
    return sys.getsizeof(object) / MB


def print_obj_size(object):
    pprint.pprint(object)
    print '{0:.2f} MB\n'.format(get_obj_size_in_mb(object))


def timeit(f):
    def inner(*args, **kwargs):
        start_time = time.time()
        result = f(*args, **kwargs)
        end_time = time.time()
        print '{} {}: {} sec.'.format(threading.current_thread().getName(), f.__name__, (end_time - start_time))
        if f.__name__ in ('process_zip_file_by_url', 'download_zip_archive_to_memory'):
            print args[0]
            print
        return result

    return inner
