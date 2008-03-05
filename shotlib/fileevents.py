#
# Wrap Gamin in twisted
#

from zope.interface import implements
from twisted.internet.interfaces import IReadDescriptor
import gamin

class FileWatcher(object):
    implements(IReadDescriptor)
    
    def __init__(self):
        self.mon = gamin.WatchMonitor()
        self.__watches = []

    def fileno(self):
        return self.mon.get_fd()

    def doRead(self):
        self.mon.handle_events()

    def connectionLost(self):
        del self.__watches
        del self.mon

    def close(self):
        for watch in self.__watches:
            self.mon.stop_watch(watch)
        del self.mon

    def addFileWatch(self, path, callback):
        self.__watches.add(path)
        return self.mon.watch_file(path, callback)

    def addDirectoryWatch(self, path, callback):
        self.__watches.add(path)
        return self.mon.watch_directory(path, callback)

WATCHER = None
def start():
    from twisted.internet import reactor
    global WATCHER
    if not WATCHER:
        WATCHER = FileWatcher()
        reactor.addReader(FileWatcher())
    return WATCHER

def stop():
    global WATCHER
    if WATCHER:
        reactor.removeReader(WATCHER)
        WATCHER.close()
        WATCHER = None
