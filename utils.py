import sys
import time
import threading
import pprint

from constants import MB

import urllib2
import zipfile
import cStringIO

def get_obj_size_in_mb(object):
    return sys.getsizeof(object) / MB


def print_obj_size(object, print_obj=False):
    if print_obj:
        pprint.pprint(object)
    print '{0:.2f} MB\n'.format(get_obj_size_in_mb(object))


def timeit(f):
    def inner(*args, **kwargs):
        start_time = time.time()
        result = f(*args, **kwargs)
        end_time = time.time()
        print '{} {}: {:.3f} sec.'.format(
            threading.current_thread().getName(), f.__name__, (end_time - start_time))
        if f.__name__ == '__download':
            print args[0].zip_file_url[-15:]
        return result

    return inner


def get_archived_file_object_from_zip_archive(zip_archive):
    zipped_files = zip_archive.namelist()
    if zipped_files:
        csv_file_name = zipped_files[0]
        return zip_archive.open(csv_file_name)
    raise Exception('Exception: zip archive contain no files!')

def download_zip_archive_to_memory(zip_file_url):
    zip_file_bytes = urllib2.urlopen(zip_file_url).read()
    buffer = cStringIO.StringIO(zip_file_bytes)
    return zipfile.ZipFile(buffer)