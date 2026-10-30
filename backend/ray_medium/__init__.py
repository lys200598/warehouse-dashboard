"""Ray execution seam for the V5 medium simulation.

V5 dispatch, reservation, inventory, energy and knowledge gates remain in the
central simulation.  Each robot's per-tick motion segment is computed by its
own Ray actor, preserving the original 100-actor shape without replacing the
V5 scheduler or changing the 120 layout/data model.
"""
