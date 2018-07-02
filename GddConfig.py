import ConfigParser
import sys

try:
    from settings import *
except ImportError:
    print "Could not import settings!"
    sys.exit(1)

class GddConfig(object):
    def __init__(self):
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(BASE + 'db_conn.cfg')
        self.job_config = ConfigParser.SafeConfigParser()
        self.job_config.read(BASE + 'jobs.cfg')
        self.defaults = self.config.defaults()

    def get(self, setting, corpus=None):
        if corpus is not None and corpus != "":
            if corpus == "public":
                corpus = "DEFAULT"
            return self.config.get(corpus, setting)
        else:
            return self.defaults[setting]

    def job_get(self, setting):
        return self.job_config.get(setting)
