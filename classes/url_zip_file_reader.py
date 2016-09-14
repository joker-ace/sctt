import zipfile
import StringIO

from url_file_reader import URLFileReader


class URLZipFileReader(URLFileReader):
    def __init__(self, file_uri):
        super(URLZipFileReader, self).__init__(file_uri)

    def get_zip_file_object(self):
        bytes = super(URLZipFileReader, self).read()
        buffer = StringIO.StringIO(bytes)
        return zipfile.ZipFile(buffer)
