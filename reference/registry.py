"""Estado do serviço de referência: ranks estáveis e leases de heartbeat."""

import time


class ServerRegistry:
    def __init__(self, timeout_seconds=30, clock=time.time):
        self.timeout_seconds = timeout_seconds
        self.clock = clock
        self._ranks = {}
        self._last_heartbeat = {}
        self._next_rank = 1

    def _expire(self):
        now = self.clock()
        expired = [name for name, seen in self._last_heartbeat.items()
                   if now - seen > self.timeout_seconds]
        for name in expired:
            del self._last_heartbeat[name]

    def register(self, name):
        self._expire()
        if name not in self._ranks:
            self._ranks[name] = self._next_rank
            self._next_rank += 1
        self._last_heartbeat[name] = self.clock()
        return self._ranks[name]

    def heartbeat(self, name):
        self._expire()
        rank = self._ranks.get(name)
        if rank is None:
            rank = self.register(name)
        self._last_heartbeat[name] = self.clock()
        return rank

    def available(self):
        self._expire()
        return [
            {"name": name, "rank": self._ranks[name]}
            for name in sorted(self._last_heartbeat, key=self._ranks.get)
        ]
