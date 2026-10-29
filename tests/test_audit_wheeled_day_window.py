import unittest

from ops.audit_wheeled_day_window import classify


class WindowClassificationTests(unittest.TestCase):
    def test_future_tasks_and_not_due_work_are_not_drain_failures(self):
        tasks = [
            {'release_second': 1, 'due_second': 10, 'completed_at': 9, 'status': 'completed'},
            {'release_second': 2, 'due_second': 20, 'completed_at': 21, 'status': 'completed'},
            {'release_second': 99, 'due_second': 120, 'status': 'carrying'},
            {'release_second': 90, 'due_second': 99, 'status': 'assigned'},
            {'release_second': 100, 'due_second': 150},
            {'release_second': 101, 'due_second': 160},
        ]
        result = classify(tasks, 100)
        self.assertEqual({'released': 4, 'completed': 2, 'completed_late': 1,
            'unfinished': 2, 'unfinished_overdue': 1, 'unfinished_not_yet_due': 1,
            'unfinished_due_at_boundary': 0, 'future_or_boundary_release': 2}, result)

    def test_due_exactly_at_boundary_is_separate_from_already_overdue(self):
        tasks = [{'release_second': 50, 'due_second': 100, 'status': 'carrying'}]
        result = classify(tasks, 100)
        self.assertEqual(0, result['unfinished_overdue'])
        self.assertEqual(1, result['unfinished_due_at_boundary'])


if __name__ == '__main__':
    unittest.main()
