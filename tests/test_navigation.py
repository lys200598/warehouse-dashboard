import math
import unittest

from backend.domain import RobotAgent,pickup_point
from backend.routing import detour,segment_hits_box
from backend.runtime import LocalRobotRuntime
from backend.simulation import SimulationCoordinator,SimulationConfig
from backend.traffic import TrafficManager,swept_distance


def movement(robot,start,end,stage='toRack'):
    return {'robot_id':robot,'from':dict(x=start[0],y=start[1]),'to':dict(x=end[0],y=end[1]),
            'moving':start!=end,'stage':stage}


class NavigationTest(unittest.TestCase):
    def test_swept_crossing_is_not_endpoint_only(self):
        proposals=[movement(1,(0,0),(2,2)),movement(2,(0,2),(2,0))]
        result=TrafficManager(.12).resolve(proposals)
        self.assertLess(sum(result.accepted.values()),2)

    def test_parked_robot_still_occupies_space(self):
        result=TrafficManager(.12).resolve([movement(1,(0,0),(2,0)),movement(2,(1,0),(1,0),'idle')])
        self.assertFalse(result.accepted[1])

    def test_rejection_propagates_to_following_mover(self):
        proposals=[movement(1,(0,0),(1,0)),movement(2,(1,0),(2,0)),movement(3,(2,0),(2,0),'placing')]
        result=TrafficManager(.12).resolve(proposals)
        self.assertFalse(result.accepted[1]);self.assertFalse(result.accepted[2])

    def test_detour_respects_shelf_footprint(self):
        boxes=[(8.,4.3,10.8,5.45)]
        path=detour((9.,2.),(9.,7.),[],.12,boxes=boxes)
        self.assertIsNotNone(path)
        for a,b in zip(path,path[1:]):
            self.assertFalse(segment_hits_box((a['x'],a['y']),(b['x'],b['y']),boxes[0],.119))

    def test_nearby_blocker_must_not_be_discarded(self):
        path=detour((1.,1.),(3.,1.),[(1.14,1.)],.12)
        self.assertIsNotNone(path)
        self.assertGreater(len(path),2)

    def test_both_layouts_have_consistent_pickup_points(self):
        edge={tuple(pickup_point(i,'edge').values()) for i in range(1,101)}
        corner={tuple(pickup_point(i,'corner').values()) for i in range(1,101)}
        self.assertEqual(len(edge),100);self.assertEqual(len(corner),8)
        self.assertTrue(all(.6<=x<=5.8 and 18.55<=y<=21.25 for x,y in corner))

    def test_batch_preserves_capacity_and_never_clears_inventory(self):
        sim=SimulationCoordinator(LocalRobotRuntime(),SimulationConfig(robot_count=3,max_active_robots=3))
        try:
            occupied=sum(s['occupied'] for r in sim.racks for s in r['slots'])
            result=sim.inject_task_batch(100)
            self.assertEqual(result['assigned'],0)
            self.assertEqual(result['pending_priority'],100)
            self.assertEqual(occupied,sum(s['occupied'] for r in sim.racks for s in r['slots']))
            for invalid in (0,101,True,1.5):
                with self.assertRaises(ValueError):sim.inject_task_batch(invalid)
        finally:sim.close()

    def test_checkpoint_uses_committed_barrier_cache(self):
        runtime=LocalRobotRuntime()
        sim=SimulationCoordinator(runtime,SimulationConfig(robot_count=3))
        original=runtime.snapshots
        runtime.snapshots=lambda: (_ for _ in ()).throw(AssertionError('redundant barrier'))
        try:
            payload=sim.checkpoint_payload()
            self.assertEqual(len(payload['robots']),3)
        finally:
            runtime.snapshots=original;sim.close()

    def test_reroute_preserves_destination_and_cannot_teleport(self):
        sim=SimulationCoordinator(LocalRobotRuntime(),SimulationConfig(robot_count=1))
        try:
            agent=sim.runtime.agents[1];s=agent.state;destination=s.path[-1]
            with self.assertRaises(ValueError):
                agent.reroute([{'x':s.x+1,'y':s.y},destination])
            with self.assertRaises(ValueError):
                agent.reroute([{'x':s.x,'y':s.y},{'x':1.,'y':1.}])
            old=(s.task_id,s.cargo_id,s.battery)
            agent.reroute([{'x':s.x,'y':s.y},destination])
            self.assertEqual(old,(s.task_id,s.cargo_id,s.battery))
        finally:sim.close()
