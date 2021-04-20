# adapted from https://stackoverflow.com/questions/3539107/python-rewinding-one-line-in-file-when-iterating-with-f-next
class RewindableFileIterator:
    def __init__(self, file_stream):
        self._file_stream = file_stream
        self.rewind()

    def __iter__(self):
        return self

    def __next__(self):
        if self._use_cached:
            self._use_cached = False
        else:
            self._cached = next(self._iter)
        return self._cached

    def step_back(self):
        if self._use_cached:
            raise RuntimeError("Cannot step back more than one step.")
        elif self._cached is None:
            raise RuntimeError("Cannot step back from beginning.")
        self._use_cached = True

    def rewind(self):
        self._file_stream.seek(0)
        self._iter = iter(self._file_stream)
        self._use_cached, self._cached = False, None


class DbRecordBlockServer:
    def __init__(self, file_stream):
        self._db_stream = RewindableFileIterator(file_stream)
        self.block_id = None
        self.data = list()
        self._read_block()
    def _read_block(self, override=False):
        while True:
            try:
                block_id, *block_data = next(self._db_stream).strip().split("\t")
            except StopIteration:
                break

            if self.block_id is None or self.block_id == block_id:
                self.data.append(block_data)
                self.block_id = block_id
            elif override:
                self.block_id, self.data = block_id, [block_data]
            else:
                self._db_stream.step_back()
                break
        return self.block_id, self.data

    def __next__(self):
        self._read_block(override=True)
        return self.block_id, self.data
    def get(self):
        return self.block_id, self.data
    def find(self):
        ...
