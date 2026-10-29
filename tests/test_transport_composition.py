import unittest
from backend.warehouse.composition import physical_classes, with_agents
from backend.knowledge_runtime.sim import KnowledgeSimulation
from backend.wheeled.sim import WheeledMixin
from backend.warehouse.__main__ import parser


class CompositionTests(unittest.TestCase):
    def test_old_default_and_opt_in(self):
        old, config = physical_classes()
        self.assertIs(old, KnowledgeSimulation)
        self.assertEqual(config().speed, .8)
        wheel, cfg = physical_classes('wheeled')
        self.assertTrue(issubclass(wheel, KnowledgeSimulation))
        self.assertTrue(issubclass(wheel, WheeledMixin))
        self.assertEqual(cfg().speed, 1.2)
        self.assertEqual(cfg().replenishment_policy, 'swap-first')
        with self.assertRaises(ValueError): physical_classes('magic')

    def test_parser_and_no_implicit_agents(self):
        args = parser().parse_args(['run', '--dataset','a', '--output','b','--run-id','r'])
        self.assertEqual(args.mobility, 'legacy')
        self.assertFalse(args.robot_agents)
        args = parser().parse_args(['run','--dataset','a','--output','b','--run-id','r',
            '--mobility','wheeled','--robot-agents'])
        self.assertTrue(args.robot_agents)
        sentinel = object()
        self.assertIs(with_agents(sentinel, output='unused'), sentinel)


if __name__ == '__main__': unittest.main()
