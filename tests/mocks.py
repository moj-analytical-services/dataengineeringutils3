class MockCursor:
    def __init__(self, total_rows, *args, **kwargs):
        self.n = 0
        self.results = range(total_rows)
        super().__init__(*args, **kwargs)

    def execute(self, *args, **kwargs):
        pass

    def fetchmany(self, fetch_size):
        n = self.n
        self.n = min(len(self.results), self.n + fetch_size)
        return self.results[n: self.n]
