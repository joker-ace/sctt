import urllib2


class URLFileReader(object):
    def __init__(self, file_uri):
        super(URLFileReader, self).__init__()
        self.file_uri = file_uri

    def read(self):
        return urllib2.urlopen(self.file_uri).read()
