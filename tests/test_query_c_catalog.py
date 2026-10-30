import unittest

from backend.query_c.catalog import ANCHOR, PACKS, QUERIES, ContextUnavailable, read_context


class Result:
    def __init__(self, rows):
        self.rows = rows
    def data(self):
        return self.rows


class FakeDriver:
    def __init__(self, latest=10, step=True):
        self.latest, self.step, self.calls = latest, step, []
    def session(self, **kwargs):
        assert kwargs["default_access_mode"] == "READ"
        return self
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def execute_read(self, function):
        return function(self)
    def run(self, query, **params):
        self.calls.append((query, params))
        if query == ANCHOR:
            return Result([{"latest_step": self.latest}])
        if query == QUERIES["step"]:
            return Result([{"step": {"tick": params["t"]}}] if self.step else [])
        return Result([])


class CatalogTests(unittest.TestCase):
    def test_latest_is_resolved_once_and_pinned_everywhere(self):
        driver = FakeDriver()
        result = read_context(driver, "run", 0, state_window=4, event_window=6)
        self.assertEqual(result["tick"], 10)
        self.assertEqual(len(driver.calls), 6)
        for _, params in driver.calls[1:]:
            self.assertEqual((params["t"], params["state_lower"], params["event_lower"]), (10, 6, 4))
        self.assertIn("self_state", result["missing"])
        self.assertEqual(set(result["packs"]), set(PACKS))
        self.assertTrue(all(p["unavailable"] for p in result["packs"].values()))

    def test_explicit_tick_works_without_latest_or_step(self):
        result = read_context(FakeDriver(None, False), "run", 1, tick=4)
        self.assertEqual(result["tick"], 4)
        self.assertIn("step", result["missing"])

    def test_latest_missing_or_uncommitted_is_error(self):
        for driver in (FakeDriver(None), FakeDriver(-1), FakeDriver(True), FakeDriver(10, False)):
            with self.assertRaises(ContextUnavailable):
                read_context(driver, "run", 1)

    def test_invalid_inputs_fail_before_io(self):
        for kwargs in ({"tick": -1}, {"tick": True}, {"state_window": 0},
                       {"event_window": 1.5}, {"radius": float("nan")},
                       {"radius": 0}, {"helper_min_battery": 101}):
            driver = FakeDriver()
            with self.assertRaises(ValueError):
                read_context(driver, "run", 1, **kwargs)
            self.assertEqual(driver.calls, [])
        for run, robot in (("", 1), ("r", -1), ("r", True), ("r", "1")):
            with self.assertRaises(ValueError):
                read_context(FakeDriver(), run, robot)

    def test_recent_queries_have_two_bounds_and_no_silent_row_cap(self):
        for name in ("recent_states", "recent_events"):
            self.assertIn(" <= ", QUERIES[name])
            self.assertIn(" > ", QUERIES[name])
            self.assertNotIn("LIMIT", QUERIES[name])
        self.assertNotIn("Execution", " ".join(QUERIES.values()))


if __name__ == "__main__":
    unittest.main()
