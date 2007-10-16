import spread
import logging
import sys

class SpreadLogHandler(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self)
        self.mbox = spread.connect('4803@localhost', membership=False)

    def emit(self, record):
        self.mbox.multicast(spread.SAFE_MESS, 'LOG', self.format(record), 0)

    def close(self):
        self.mbox.disconnect()
        del self.mbox


def configure_logging():
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    console = logging.StreamHandler(sys.stdout)
    l = logging.getLogger()
    console.setFormatter(formatter)
    l.addHandler(console)
    l.setLevel(logging.DEBUG)
