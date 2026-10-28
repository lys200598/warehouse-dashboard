# Warehouse Graph Contract v1

## Purpose

This contract is the boundary between the warehouse-layout producer and the knowledge-graph implementation. The layout files are authoritative for physical entities; the graph importer validates and persists them. Frontend rendering is not an authority for IDs or coordinates.

## Delivery package

Each dataset package contains:

| File | Required content |
| --- | --- |
| `layout.json` | layout specification and rack geometry |
| `task-locations.csv` | every slot and inbound/outbound station |
| `initial_inventory.csv` | `goods_id,location_id` |
| `tasks.csv` | `task_id,task_type,goods_id,release_time,due_time,source_id,target_id` |
| `manifest.json` | versions, SHA-256 hashes, counts and generation assumptions |

`task_type` is exactly `INBOUND` or `OUTBOUND`. Times are ISO-8601 with timezone. `location_id` is immutable inside one layout version.

## IDs and versions

- `Warehouse.id`: `warehouse:<layout.spec.version>`.
- `Rack.id`: `<warehouse-id>:rack:<rack-id>`.
- `Location.id`: `<warehouse-id>:location:<location-id>`.
- `Dataset.id`: `dataset:<manifest.version>:<manifest.start YYYY-MM-DD>`; a changed layout or data format requires a new manifest version.
- `Goods.id`: `<dataset-id>:goods:<goods-id>`.
- `WarehouseTask.id`: `<dataset-id>:task:<task-id>`.
- Runtime IDs are scoped to a run: `<run-id>:robot:<robot-id>`, `<run-id>:execution:<task-id>`, and `<run-id>:robot:<robot-id>:state:<tick>`.

## Graph model

Static graph:

```text
Dataset -[:USES_LAYOUT]-> Warehouse -[:HAS_ZONE]-> Zone -[:HAS_RACK]-> Rack
Warehouse -[:HAS_LOCATION]-> Location
Rack -[:HAS_SLOT]-> Location(kind=slot)
Dataset -[:INCLUDES_GOODS]-> Goods -[:INITIAL_AT]-> Location
Dataset -[:INCLUDES_TASK]-> WarehouseTask -[:CONCERNS]-> Goods
WarehouseTask -[:FROM]-> Location
WarehouseTask -[:TO]-> Location
```

Runtime graph:

```text
Run -[:USES_DATASET]-> Dataset
Robot -[:IN_RUN]-> Run
AgentState {run_id,tick} -[:IN_RUN]-> Run; AgentState -[:OF_ROBOT]-> Robot
Execution -[:IN_RUN]-> Run; Execution -[:ASSIGNED_TO]-> Robot
Execution -[:EXECUTES]-> WarehouseTask
RuntimeEvent -[:IN_RUN]-> Run; RuntimeEvent -[:ABOUT_ROBOT]-> Robot; RuntimeEvent -[:ABOUT_EXECUTION]-> Execution
```

`EXECUTES` is written only when the configured dataset ID and task ID match a static task; unmatched legacy tasks must not create a synthetic static task.

`Execution` is the canonical runtime lifecycle record. Assignment, pickup and completion timestamps are written from `RuntimeEvent`; a reverse `Robot-[:ASSIGNED]->Execution` relationship is deliberately not required because Neo4j relationships are traversable in both directions. Runtime task reads use `dataset_id + task_id`; replay reads use `run_id + tick`.

## Import validation

Before Neo4j is contacted, the importer must validate the layout SHA-256 and task/inventory SHA-256 values in `manifest.json`, unique IDs, slot capacity, inventory capacity, location references, supported task types and required columns. A failed validation must write nothing.

## Ownership

- Layout collaborator: owns `layout.json`, `task-locations.csv`, their version and physical IDs.
- Dataset producer: owns inventory/task contents and manifest hashes.
- Knowledge-graph owner: owns this contract, validation, Neo4j schema/import, runtime persistence and graph APIs.
- Frontend owner: consumes layout data but does not redefine physical IDs.
