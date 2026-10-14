"""Testes de contrato da Parte 3 para ranks e disponibilidade."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "reference"))
from registry import ServerRegistry


class RegistryTests(unittest.TestCase):
    def test_registration_assigns_unique_stable_ranks(self):
        now = [100.0]
        registry = ServerRegistry(timeout_seconds=30, clock=lambda: now[0])
        first = registry.register("python-server")
        second = registry.register("java-server")
        repeated = registry.register("python-server")
        self.assertEqual((first, second, repeated), (1, 2, 1))
        self.assertEqual(registry.available(), [
            {"name": "python-server", "rank": 1},
            {"name": "java-server", "rank": 2},
        ])

    def test_heartbeat_renews_availability_and_expiry_removes_server(self):
        now = [100.0]
        registry = ServerRegistry(timeout_seconds=30, clock=lambda: now[0])
        registry.register("python-server")
        now[0] = 120.0
        registry.heartbeat("python-server")
        now[0] = 140.0
        self.assertEqual(len(registry.available()), 1)
        now[0] = 151.0
        self.assertEqual(registry.available(), [])
        self.assertEqual(registry.register("python-server"), 1)

    def test_heartbeat_can_register_server_if_registration_was_lost(self):
        registry = ServerRegistry(timeout_seconds=30, clock=lambda: 100.0)
        self.assertEqual(registry.heartbeat("java-server"), 1)
        self.assertEqual(registry.available(), [{"name": "java-server", "rank": 1}])


if __name__ == "__main__":
    unittest.main()
