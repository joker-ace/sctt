import sys
import time
import os

from constants import MB

import urllib2
import zipfile
import cStringIO


def get_obj_size_in_mb(object):
    return sys.getsizeof(object) / MB


def print_obj_size(object, print_obj=False, message=''):
    print '{} {}{:.2f} MB'.format(os.getpid(), message, get_obj_size_in_mb(object))


def timeit(f):
    def inner(*args, **kwargs):
        start_time = time.time()
        result = f(*args, **kwargs)
        end_time = time.time()
        if f.__name__ == '__download':
            template = '{} {}({}): {:.3f} sec.'
            template_args = (os.getegid(), f.__name__, args[0].zip_file_url[-15:], (end_time - start_time))
        else:
            template = '{} {}: {:.3f} sec.'
            template_args = (os.getegid(), f.__name__, (end_time - start_time))

        print template.format(*template_args)

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


@timeit
def merge_statistics(list_of_dicts):
    merged_statistics = {}
    get = merged_statistics.get
    for stat in list_of_dicts:
        for k, v in stat.iteritems():
            merged_statistics[k] = get(k, 0.0) + v
    print_obj_size(merged_statistics, message='Merged statistics dict size: ')
    return merged_statistics
