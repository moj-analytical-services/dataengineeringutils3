class MockCursor:
    def __init__(self, results, description=[]):
        self.n = 0
        self.results = results
        self.description = description

    def execute(self, *args, **kwargs):
        pass

    def fetchmany(self, fetch_size):
        n = self.n
        self.n = min(len(self.results), self.n + fetch_size)
        return self.results[n: self.n]
