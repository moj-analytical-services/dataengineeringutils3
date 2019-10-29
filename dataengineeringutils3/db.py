class SelectQuerySet:
    """
    Iterator for fetching select query results in chunks
    """
    def __init__(self, cursor, fetch_size, select_query, **query_kwargs):
        self.query = select_query
        self.cursor = cursor
        self.fetch_size = fetch_size
        self._result_cache = None
        self.cursor.execute(select_query, **query_kwargs)

    def __iter__(self):
        self.n = 0
        return self

    def __next__(self):
        if self._result_cache is not None:
            try:
                val = next(self._result_cache)
                self.n += 1
                return val
            except StopIteration:
                pass
        results = self.cursor.fetchmany(self.fetch_size)
        if len(results) == 0:
            raise StopIteration()
        self._result_cache = iter(results)
        val = next(self._result_cache)
        self.n += 1
        return val

    @property
    def headers(self):
        return [c[0] for c in self.cursor.description]
