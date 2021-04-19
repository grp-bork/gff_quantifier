# adapted from https://stackoverflow.com/questions/3539107/python-rewinding-one-line-in-file-when-iterating-with-f-next
class RewindableFileIterator:
    not_started = None

    def __init__(self, file_stream):
        self._file_stream = file_stream
        self._iter = iter(self._file_stream)
        self._use_save = False
        self._save = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._use_save:
            self._use_save = False
        else:
            self._save = next(self._iter)
        return self._save

    def step_back(self):
        if self._use_save:
            raise RuntimeError("Tried to rewind more than one step.")
        elif self._save is None:
            raise RuntimeError("Can't rewind past the beginning.")
        self._use_save = True

    def rewind(self):
        self._file_stream.seek(0)
        self._iter = iter(self._file_stream)
        self._use_save = False
        self._save = None
