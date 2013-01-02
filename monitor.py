import pyinotify
import sys

watchdir = '/home/ts3e11/test/data'

class MyEventHandler(pyinotify.ProcessEvent):
    def my_init(self, file_object=sys.stdout):
        self._file_object = file_object
        self.active_new_files = {}

    def process_IN_CREATE(self, event):
        self._file_object.write("File transfer started: %s\n" % event.pathname)
        self._file_object.flush()
        self.active_new_files[event.pathname] = None

    def process_IN_CLOSE_WRITE(self, event):
        if event.pathname in self.active_new_files:
            self._file_object.write('New file transfer finished: %s\n' %
                                    event.pathname)
            self._file_object.flush()
            self.active_new_files.pop(event.pathname)

        else:
            self._file_object.write("Old file modified: %s\n" % event.pathname)
            self._file_object.flush()


wm = pyinotify.WatchManager()
notifier = pyinotify.Notifier(wm, MyEventHandler())
mask = pyinotify.IN_CREATE | pyinotify.IN_CLOSE_WRITE
wm.add_watch(watchdir, mask, rec=True)

notifier.loop()

