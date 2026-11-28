"""A serialized local-file session with sliding-window, epoch-based adaptation."""
from collections import deque
from pathlib import Path
from threading import RLock

from .engine import CertificateIndex, Workload, build_index_report, execute, execute_sql


class CertSkip:
    """Manage an index and refresh it before the first query of each new epoch.

    Successful typed queries enter a bounded recent-query window. Refreshes build
    a replacement completely before publishing it. A failed refresh leaves the
    previous index and pending observations intact and raises before execution.
    Calls on one session are serialized; files must still be immutable.
    """
    def __init__(self, paths, *, memory_budget=10 * 1024 * 1024, epoch_size=1000,
                 window_size=5000, candidate_limit=1000, candidate_strategy="merged",
                 policy="overlap", batch_size=65536):
        self.paths = tuple(sorted({str(Path(p).resolve()) for p in paths}))
        if not self.paths:
            raise ValueError("At least one Parquet file is required")
        for name, value in (("epoch_size", epoch_size), ("window_size", window_size),
                            ("batch_size", batch_size)):
            if type(value) is not int or value <= 0:
                raise ValueError(name + " must be a positive integer")
        if type(candidate_limit) is not int or candidate_limit < 0:
            raise ValueError("Candidate limit must be nonnegative")
        if policy not in ("overlap", "frequency") or candidate_strategy not in ("queries", "merged"):
            raise ValueError("Invalid policy or candidate strategy")
        self._index = CertificateIndex()
        if type(memory_budget) is not int or memory_budget < self._index.memory_bytes:
            raise ValueError("Budget must fit the empty index")
        self.epoch_size = epoch_size
        self.options = dict(memory_budget=memory_budget, candidate_limit=candidate_limit,
                            candidate_strategy=candidate_strategy, policy=policy, batch_size=batch_size)
        self._history = deque(maxlen=window_size)
        self._pending = 0
        self._lock = RLock()
        self.last_build = None
        self.refresh_count = 0

    @property
    def index(self):
        return self._index

    @property
    def workload(self):
        with self._lock:
            workload = Workload()
            for query in self._history:
                workload.observe(query)
            return workload

    def train(self, workload):
        """Build from an explicit workload; subsequent adaptation uses recent queries."""
        with self._lock:
            report = build_index_report(self.paths, workload, **self.options)
            self._index = report.index
            self.last_build = report.stats
            self._pending = 0
            self.refresh_count += 1
            return report

    def refresh(self):
        with self._lock:
            return self.train(self.workload)

    def execute(self, query):
        with self._lock:
            if self._pending >= self.epoch_size:
                self.refresh()
            result = execute(self.paths, query, self._index)
            self._history.append(query)
            self._pending += 1
            return result

    def execute_sql(self, sql):
        """Ordinary DuckDB fallback; unsupported SQL is not learned as a Region."""
        with self._lock:
            return execute_sql(self.paths, sql)
