class MockCursor:
    """Mocks cursor object for testing fetchmany db results"""
    def __init__(self, length, description=[]):
        self.n = 0
        self.length = length
        self.description = description

    def _get_results(self, n):
        return [
            '{"uuid": "fkjherpiutrgponfevpoir3qjgp8prueqhf9pq34hf89hwfpu92q"}'
        ] * n

    def execute(self, *args, **kwargs):
        pass

    def fetchmany(self, fetch_size):
        if self.n > self.length:
            raise StopIteration()
        self.n += fetch_size
        if self.n > self.length:
            fetch_size -= (self.n - self.length)
            self.n = self.length
        return self._get_results(fetch_size)


class MockQs:
    def __init__(self, results):
        self.results = results
        self.returned = False

    def execute(self, *args, **kwargs):
        pass

    def fetchmany(self, *args, **kwargs):
        if not self.returned:
            self.returned = True
            return self.results
        raise StopIteration()
