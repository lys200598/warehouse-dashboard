(() => {
  "use strict";

  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

  const canvas = $("#warehouseCanvas");
  const stage = $("#canvasStage");
  const ctx = canvas.getContext("2d");
  const portraitCanvas = $("#robotPortrait");
  const portraitCtx = portraitCanvas.getContext("2d");
  const WORLD = { width: 32, height: 22 };

  const COLORS = {
    lime: "#c9f46c",
    blue: "#68c7ff",
    orange: "#f0a65a",
    red: "#ff6b64",
    idle: "#798681",
    floor: "#111a18",
    grid: "#26322f",
    gridStrong: "#34423e",
    text: "#a9b6b1",
    rack: "#4c5d57",
    rackDark: "#293632",
    box: "#a67546",
  };

  const SHELF_LEVELS = [
    { level: 1, baseZ: 0.07, centerHeight: 0.28 },
    { level: 2, baseZ: 0.61, centerHeight: 0.82 },
    { level: 3, baseZ: 1.15, centerHeight: 1.36 },
  ];
  const SHELF_GAP = 0.54;
  const RACK_HEIGHT = 1.78;
  const SHELF_COLUMNS = 6;
  const SHELF_DEPTH_ROWS = 2;
  const PACKAGE_WIDTH = 0.28;
  const PACKAGE_DEPTH = 0.24;
  const PACKAGE_HEIGHT = 0.21;
  const ROBOT_TYPES = [
    { key: "A1", name: "基础型 A1", maxLevel: 1, maxHeight: 0.55, extension: 0.18, accent: "#d6e0dc" },
    { key: "R2", name: "增程型 R2", maxLevel: 2, maxHeight: 1.05, extension: 0.52, accent: "#c9f46c" },
    { key: "X3", name: "长臂型 X3", maxLevel: 3, maxHeight: 1.55, extension: 0.92, accent: "#68c7ff" },
  ];
  const BOX_PALETTES = [
    { top: "#c0905f", left: "#8c6039", right: "#a57143" },
    { top: "#7399a3", left: "#45656e", right: "#547b84" },
    { top: "#91836b", left: "#635945", right: "#776a52" },
    { top: "#7b9361", left: "#52633f", right: "#63784c" },
  ];
  const ACTIVE_BOX_PALETTE = { top: "#e8c57c", left: "#9d7042", right: "#bd8952" };

  const state = {
    running: true,
    speed: 1,
    view: "iso",
    zoom: 1,
    panX: 0,
    panY: 0,
    cameraYaw: Math.PI / 4,
    cameraPitch: Math.PI / 6,
    filter: "all",
    level: "all",
    routes: true,
    heat: false,
    selectedId: 37,
    elapsed: 14 * 60 + 20,
    completed: 1284,
    flowRate: 48,
    fps: 60,
  };

  let robots = [];
  let racks = [];
  let renderWidth = 0;
  let renderHeight = 0;
  let frameCount = 0;
  let fpsTime = performance.now();
  let lastTime = performance.now();
  let domAccumulator = 0;
  let eventAccumulator = 0;
  let toastTimer = 0;
  let backendConnected = false;
  let backendAuthoritative = false;
  let backendSocket = null;
  let backendReconnectTimer = 0;
  let backendLatency = null;
  let backendSnapshotAt = 0;
  let backendInterpolationDuration = 250;
  let isDragging = false;
  let dragMoved = false;
  let pointerStart = { x: 0, y: 0, panX: 0, panY: 0, yaw: 0, pitch: 0, mode: "orbit" };

  const taskEvents = [
    { time: "08:14:32", robot: "HR-063", text: "已完成 A-08 / L2 上架" },
    { time: "08:14:18", robot: "HR-024", text: "抵达入库台，开始识别货物" },
    { time: "08:13:56", robot: "HR-081", text: "路径已重规划，绕行通道 06" },
    { time: "08:13:41", robot: "HR-012", text: "已完成 C-15 / L3 上架" },
  ];

  function mulberry32(seed) {
    return function random() {
      let t = (seed += 0x6d2b79f5);
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  const random = mulberry32(20261013);
  const pick = (items) => items[Math.floor(random() * items.length)];
  const clamp = (value, min, max) => Math.max(min, Math.min(max, value));
  const lerp = (from, to, amount) => from + (to - from) * clamp(amount, 0, 1);
  const pad = (value, size = 2) => String(value).padStart(size, "0");
  const robotCode = (id) => `HR-${pad(id, 3)}`;
  const taskCode = (robot) => robot.taskId || `TK-${2000 + ((robot.id * 37 + robot.taskRound * 11) % 780)}`;
  const cargoCode = (robot) => robot.cargoId || `BX-${4000 + ((robot.id * 83 + robot.taskRound * 17) % 890)}`;
  const shelfRowName = (depthRow = 0) => depthRow === 0 ? "后排" : "前排";

  function robotTypeFor(id) {
    const group = id % 10;
    if (group < 4) return ROBOT_TYPES[0];
    if (group < 8) return ROBOT_TYPES[1];
    return ROBOT_TYPES[2];
  }

  function categoryFor(robot) {
    if (robot.stage === "warning" || robot.blocked) return "warning";
    if (robot.stage === "picking" || robot.stage === "placing") return "handling";
    if (robot.stage === "idle" || robot.stage === "charging") return "idle";
    return "moving";
  }

  function stageLabel(robot) {
    if (robot.blocked && robot.alert) return robot.alert;
    const labels = {
      toPick: "前往入库台",
      picking: "正在取货",
      toRack: `前往 ${robot.rack?.zone || "-"}-${pad(robot.rack?.id || 0)} / L${robot.level} / C${robot.bay} / ${shelfRowName(robot.depthRow)}`,
      placing: `${robot.type.name} 伸臂放置至 L${robot.level} / C${robot.bay} / ${shelfRowName(robot.depthRow)}`,
      returning: "返回调度区",
      idle: "待命中",
      charging: "充电中",
      warning: robot.alert || "等待路径释放",
    };
    return labels[robot.stage] || "执行任务";
  }

  function createRacks() {
    const result = [];
    let rackId = 1;
    [8, 13, 18, 23, 28].forEach((x, column) => {
      [4.3, 9.4, 14.5].forEach((y, row) => {
        const slots = [];
        for (let level = 1; level <= 3; level += 1) {
          for (let depthRow = 0; depthRow < SHELF_DEPTH_ROWS; depthRow += 1) {
            for (let bay = 0; bay < SHELF_COLUMNS; bay += 1) {
              const occupied = random() > 0.48;
              slots.push({
                rackId,
                level,
                bay,
                depthRow,
                occupied,
                reservedBy: null,
                cargoId: occupied ? `ST-${pad(rackId)}${level}${bay + 1}${depthRow + 1}` : null,
                colorIndex: Math.floor(random() * 4),
                placedAt: null,
              });
            }
          }
        }
        result.push({
          id: rackId,
          zone: String.fromCharCode(65 + row),
          x,
          y,
          width: 2.8,
          depth: 1.15,
          column,
          row,
          slots,
        });
        rackId += 1;
      });
    });
    return result;
  }

  function pickupPoint(id) {
    return { x: 2.2 + (id % 4) * 0.9, y: 19.7 + (id % 2) * 0.45 };
  }

  function outboundPoint(id) {
    return { x: 2.2 + (id % 3) * 1.1, y: 2.15 + (id % 2) * 0.5 };
  }

  function rackTarget(robot) {
    const rack = robot.rack;
    const geometry = slotGeometry(rack, robot.slot);
    return { x: geometry.x + geometry.width / 2, y: rack.y + 1.72 };
  }

  function slotGeometry(rack, slot) {
    const level = SHELF_LEVELS[slot.level - 1];
    const cellWidth = (rack.width - 0.2) / SHELF_COLUMNS;
    const depthRow = slot.depthRow || 0;
    const rowTravel = rack.depth - 0.2 - PACKAGE_DEPTH;
    return {
      x: rack.x + 0.1 + slot.bay * cellWidth + (cellWidth - PACKAGE_WIDTH) / 2,
      y: rack.y + 0.1 + depthRow * rowTravel,
      width: PACKAGE_WIDTH,
      depth: PACKAGE_DEPTH,
      height: PACKAGE_HEIGHT,
      baseZ: level.baseZ,
      centerHeight: level.baseZ + PACKAGE_HEIGHT / 2,
    };
  }

  function isSlotAvailable(slot) {
    return !slot.occupied && slot.reservedBy === null;
  }

  function isSlotRecyclable(slot) {
    return slot.occupied && slot.reservedBy === null;
  }

  function buildRoute(from, destination, kind, id) {
    const laneOffset = (id % 5) * 0.16;
    const path = [{ x: from.x, y: from.y }];
    if (kind === "pickup") {
      path.push({ x: 5.5 + laneOffset, y: from.y });
      path.push({ x: 5.5 + laneOffset, y: 19.4 + laneOffset });
      path.push({ x: destination.x, y: 19.4 + laneOffset });
    } else if (kind === "rack") {
      path.push({ x: 5.6 + laneOffset, y: from.y });
      path.push({ x: 5.6 + laneOffset, y: destination.y });
      path.push({ x: destination.x, y: destination.y });
    } else {
      path.push({ x: from.x, y: 2.8 + laneOffset });
      path.push({ x: destination.x, y: 2.8 + laneOffset });
      path.push({ x: destination.x, y: destination.y });
    }
    return compactPath(path);
  }

  function compactPath(path) {
    return path.filter((point, index) => {
      if (index === 0) return true;
      const previous = path[index - 1];
      return Math.hypot(point.x - previous.x, point.y - previous.y) > 0.08;
    });
  }

  function prepareTask(robot) {
    if (robot.slot?.reservedBy === robot.id && !robot.slot.occupied) {
      robot.slot.reservedBy = null;
    }
    robot.taskRound += 1;
    let available = racks.flatMap((rack) => rack.slots
      .filter((slot) => slot.level <= robot.type.maxLevel && isSlotAvailable(slot))
      .map((slot) => ({ rack, slot })));
    const preferredLevelSlots = available.filter(({ slot }) => slot.level === robot.type.maxLevel);
    if (preferredLevelSlots.length) available = preferredLevelSlots;

    if (!available.length) {
      const recyclable = racks.flatMap((rack) => rack.slots
        .filter((slot) => slot.level <= robot.type.maxLevel && isSlotRecyclable(slot))
        .map((slot) => ({ rack, slot })))
        .sort((a, b) => (a.slot.placedAt ?? -1) - (b.slot.placedAt ?? -1));
      const released = recyclable[0];
      if (released) {
        released.slot.occupied = false;
        released.slot.cargoId = null;
        released.slot.placedAt = null;
        available = [released];
      }
    }

    const target = available[(robot.id * 7 + robot.taskRound * 5) % Math.max(1, available.length)]
      || { rack: racks[0], slot: racks[0].slots.find((slot) => slot.level <= robot.type.maxLevel) };
    robot.rack = target.rack;
    robot.slot = target.slot;
    robot.slot.reservedBy = robot.id;
    robot.level = robot.slot.level;
    robot.bay = robot.slot.bay + 1;
    robot.depthRow = robot.slot.depthRow || 0;
    robot.taskId = `TK-${2000 + ((robot.id * 37 + robot.taskRound * 11) % 780)}`;
    robot.cargoId = `BX-${4000 + ((robot.id * 83 + robot.taskRound * 17) % 890)}`;
    robot.priority = pick(["normal", "normal", "normal", "medium"]);
  }

  function setMovingStage(robot, stageName) {
    robot.stage = stageName;
    let destination;
    let kind;
    if (stageName === "toPick") {
      destination = pickupPoint(robot.id);
      kind = "pickup";
    } else if (stageName === "toRack") {
      destination = rackTarget(robot);
      kind = "rack";
    } else {
      destination = outboundPoint(robot.id);
      kind = "outbound";
    }
    robot.path = buildRoute(robot, destination, kind, robot.id);
    robot.pathIndex = 1;
    robot.stageDistance = routeLength(robot.path);
    robot.distanceTravelled = 0;
  }

  function routeLength(path) {
    let total = 0;
    for (let index = 1; index < path.length; index += 1) {
      total += Math.hypot(path[index].x - path[index - 1].x, path[index].y - path[index - 1].y);
    }
    return total;
  }

  function moveRobotAlongPath(robot, amount) {
    while (amount > 0 && robot.pathIndex < robot.path.length) {
      const target = robot.path[robot.pathIndex];
      const dx = target.x - robot.x;
      const dy = target.y - robot.y;
      const distance = Math.hypot(dx, dy);
      if (distance < 0.015) {
        robot.x = target.x;
        robot.y = target.y;
        robot.pathIndex += 1;
        continue;
      }
      const step = Math.min(amount, distance);
      robot.x += (dx / distance) * step;
      robot.y += (dy / distance) * step;
      robot.heading = Math.atan2(dy, dx);
      robot.distanceTravelled += step;
      amount -= step;
      if (step >= distance - 0.001) robot.pathIndex += 1;
    }
    return robot.pathIndex >= robot.path.length;
  }

  function createRobot(id) {
    const laneY = pick([2.8, 7.5, 12.6, 17.8, 19.6]);
    const type = robotTypeFor(id);
    const robot = {
      id,
      type,
      x: 2 + random() * 27,
      y: laneY + (random() - 0.5) * 0.35,
      heading: 0,
      stage: "idle",
      speed: 0.86 + random() * 0.36,
      battery: 42 + random() * 56,
      carrying: false,
      actionTimer: 0,
      actionDuration: 1,
      taskRound: Math.floor(random() * 9),
      level: 1,
      bay: 1,
      depthRow: 0,
      rack: racks[0],
      slot: null,
      taskId: null,
      cargoId: null,
      priority: "normal",
      alert: "",
      path: [],
      pathIndex: 0,
      stageDistance: 1,
      distanceTravelled: 0,
      phase: random() * Math.PI * 2,
      idleTimer: 1 + random() * 5,
    };

    prepareTask(robot);

    if (id <= 72) {
      if (id % 3 === 0) {
        robot.carrying = true;
        robot.x = pickupPoint(id).x;
        robot.y = pickupPoint(id).y;
        setMovingStage(robot, "toRack");
      } else if (id % 3 === 1) {
        setMovingStage(robot, "toPick");
      } else {
        robot.carrying = false;
        setMovingStage(robot, "returning");
      }
      const warmup = random() * Math.max(0.5, robot.stageDistance * 0.74);
      moveRobotAlongPath(robot, warmup);
    } else if (id <= 90) {
      robot.stage = id % 2 ? "picking" : "placing";
      robot.carrying = robot.stage === "placing";
      const point = robot.stage === "picking" ? pickupPoint(id) : rackTarget(robot);
      robot.x = point.x;
      robot.y = point.y;
      robot.actionDuration = robot.stage === "placing" ? 4.4 : 2.2 + random() * 0.8;
      robot.actionTimer = random() * robot.actionDuration;
    } else if (id <= 97) {
      robot.stage = id % 2 ? "idle" : "charging";
      if (robot.stage === "charging") {
        robot.x = 28.2 + (id % 3) * 0.7;
        robot.y = 19.2;
        robot.battery = 28 + random() * 26;
      }
    } else {
      robot.stage = "warning";
      robot.x = 7.2 + (id - 98) * 7.8;
      robot.y = 7.25 + (id % 2) * 5.1;
      robot.battery = id === 99 ? 9 : 34 + random() * 20;
      robot.alert = id === 99 ? "低电量等待调度" : id === 100 ? "路径暂时阻塞" : "站点校验中";
      robot.actionTimer = 12 + random() * 14;
    }
    return robot;
  }

  function initializeSimulation() {
    racks = createRacks();
    robots = Array.from({ length: 100 }, (_, index) => createRobot(index + 1));
    state.elapsed = 14 * 60 + 20;
    state.completed = 1284;
    state.selectedId = robots.find((robot) => robot.stage === "placing" && robot.type.maxLevel === 3 && robot.level === 3)?.id
      || robots.find((robot) => robot.stage === "placing" && robot.type.maxLevel === 3)?.id
      || 37;
    lastTime = performance.now();
    updateAllDom(true);
  }

  function updateRobot(robot, delta) {
    robot.phase += delta * (4.5 + robot.speed * 2.2);
    const moving = ["toPick", "toRack", "returning"].includes(robot.stage);

    if (moving) {
      const arrived = moveRobotAlongPath(robot, delta * robot.speed);
      robot.battery = Math.max(4, robot.battery - delta * 0.0035);
      if (arrived) {
        if (robot.stage === "toPick") {
          robot.stage = "picking";
          robot.actionDuration = 2.1 + random();
          robot.actionTimer = 0;
        } else if (robot.stage === "toRack") {
          robot.stage = "placing";
          robot.actionDuration = 4.4;
          robot.actionTimer = 0;
        } else {
          robot.stage = robot.battery < 28 ? "charging" : "idle";
          robot.idleTimer = 1.5 + random() * 5;
          if (robot.stage === "charging") {
            robot.x = 28.3 + (robot.id % 3) * 0.65;
            robot.y = 19.2;
          }
        }
      }
      return;
    }

    if (robot.stage === "picking" || robot.stage === "placing") {
      robot.actionTimer += delta;
      if (robot.actionTimer >= robot.actionDuration) {
        if (robot.stage === "picking") {
          robot.carrying = true;
          setMovingStage(robot, "toRack");
        } else {
          robot.carrying = false;
          if (robot.slot) {
            robot.slot.occupied = true;
            robot.slot.reservedBy = null;
            robot.slot.cargoId = cargoCode(robot);
            robot.slot.colorIndex = (robot.id + robot.taskRound) % BOX_PALETTES.length;
            robot.slot.placedAt = state.elapsed;
          }
          state.completed += 1;
          pushEvent(robot, `已完成 ${robot.rack.zone}-${pad(robot.rack.id)} / C${robot.bay} / ${shelfRowName(robot.depthRow)} / L${robot.level} 伸臂上架`);
          setMovingStage(robot, "returning");
        }
      }
      return;
    }

    if (robot.stage === "charging") {
      robot.battery = Math.min(100, robot.battery + delta * 2.8);
      if (robot.battery > 88) {
        prepareTask(robot);
        setMovingStage(robot, "toPick");
      }
      return;
    }

    if (robot.stage === "warning") {
      robot.actionTimer -= delta;
      if (robot.actionTimer <= 0 && robot.id !== 99) {
        robot.alert = "";
        prepareTask(robot);
        setMovingStage(robot, "toPick");
      }
      return;
    }

    robot.idleTimer -= delta;
    if (robot.idleTimer <= 0) {
      prepareTask(robot);
      setMovingStage(robot, "toPick");
    }
  }

  function pushEvent(robot, text) {
    const time = formatClock(state.elapsed);
    taskEvents.unshift({ time, robot: robotCode(robot.id), text });
    taskEvents.splice(6);
    renderLatestEvent();
  }

  function formatClock(elapsedSeconds) {
    const base = 8 * 3600 + Math.floor(elapsedSeconds);
    const hours = Math.floor(base / 3600) % 24;
    const minutes = Math.floor((base % 3600) / 60);
    const seconds = base % 60;
    return `${pad(hours)}:${pad(minutes)}:${pad(seconds)}`;
  }

  function resizeCanvas() {
    const bounds = stage.getBoundingClientRect();
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    renderWidth = Math.max(1, Math.round(bounds.width));
    renderHeight = Math.max(1, Math.round(bounds.height));
    canvas.width = Math.round(renderWidth * dpr);
    canvas.height = Math.round(renderHeight * dpr);
    canvas.style.width = `${renderWidth}px`;
    canvas.style.height = `${renderHeight}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  function projectionMetrics() {
    if (state.view === "top") {
      const baseScale = Math.min((renderWidth - 76) / WORLD.width, (renderHeight - 56) / WORLD.height);
      const scale = baseScale * state.zoom;
      return {
        scale,
        originX: (renderWidth - WORLD.width * scale) / 2 + state.panX,
        originY: (renderHeight - WORLD.height * scale) / 2 + state.panY,
      };
    }

    const cosYaw = Math.cos(state.cameraYaw);
    const sinYaw = Math.sin(state.cameraYaw);
    const sinPitch = Math.sin(state.cameraPitch);
    const cosPitch = Math.cos(state.cameraPitch);
    const rotatedWidth = Math.abs(WORLD.width * cosYaw) + Math.abs(WORLD.height * sinYaw);
    const rotatedDepth = Math.abs(WORLD.width * sinYaw) + Math.abs(WORLD.height * cosYaw);
    const maxSceneZ = 2.2;
    const fitWidth = (renderWidth - 60) / Math.max(1, rotatedWidth);
    const fitHeight = (renderHeight - 90) / Math.max(1, rotatedDepth * sinPitch + maxSceneZ * cosPitch * 1.3);
    const scale = Math.min(fitWidth, fitHeight) * state.zoom;
    const verticalReserve = maxSceneZ * scale * cosPitch * 1.3;
    return {
      scale,
      cosYaw,
      sinYaw,
      sinPitch,
      cosPitch,
      originX: renderWidth / 2 + state.panX,
      originY: renderHeight / 2 + verticalReserve / 2 + state.panY,
    };
  }

  function project(x, y, z = 0) {
    const metrics = projectionMetrics();
    if (state.view === "top") {
      return { x: metrics.originX + x * metrics.scale, y: metrics.originY + y * metrics.scale };
    }
    const worldX = x - WORLD.width / 2;
    const worldY = y - WORLD.height / 2;
    const viewX = worldX * metrics.cosYaw - worldY * metrics.sinYaw;
    const viewDepth = worldX * metrics.sinYaw + worldY * metrics.cosYaw;
    return {
      x: metrics.originX + viewX * metrics.scale,
      y: metrics.originY + viewDepth * metrics.scale * metrics.sinPitch - z * metrics.scale * metrics.cosPitch * 1.3,
    };
  }

  function cameraDepth(x, y) {
    if (state.view === "top") return y;
    return (x - WORLD.width / 2) * Math.sin(state.cameraYaw)
      + (y - WORLD.height / 2) * Math.cos(state.cameraYaw);
  }

  function pathPolygon(points, fill, stroke, lineWidth = 1) {
    if (!points.length) return;
    ctx.beginPath();
    ctx.moveTo(points[0].x, points[0].y);
    points.slice(1).forEach((point) => ctx.lineTo(point.x, point.y));
    ctx.closePath();
    if (fill) {
      ctx.fillStyle = fill;
      ctx.fill();
    }
    if (stroke) {
      ctx.strokeStyle = stroke;
      ctx.lineWidth = lineWidth;
      ctx.stroke();
    }
  }

  function drawFloor() {
    const corners = [project(0, 0), project(WORLD.width, 0), project(WORLD.width, WORLD.height), project(0, WORLD.height)];
    pathPolygon(corners, COLORS.floor, "#3a4844", 1);

    ctx.save();
    ctx.lineWidth = 0.55;
    for (let x = 0; x <= WORLD.width; x += 1) {
      const strong = x % 5 === 0;
      const a = project(x, 0);
      const b = project(x, WORLD.height);
      ctx.beginPath();
      ctx.moveTo(a.x, a.y);
      ctx.lineTo(b.x, b.y);
      ctx.strokeStyle = strong ? COLORS.gridStrong : COLORS.grid;
      ctx.globalAlpha = strong ? 0.72 : 0.42;
      ctx.stroke();
    }
    for (let y = 0; y <= WORLD.height; y += 1) {
      const strong = y % 5 === 0;
      const a = project(0, y);
      const b = project(WORLD.width, y);
      ctx.beginPath();
      ctx.moveTo(a.x, a.y);
      ctx.lineTo(b.x, b.y);
      ctx.strokeStyle = strong ? COLORS.gridStrong : COLORS.grid;
      ctx.globalAlpha = strong ? 0.72 : 0.42;
      ctx.stroke();
    }
    ctx.restore();

    drawFloorZone(0.6, 18.55, 5.2, 2.7, "rgba(201,244,108,0.075)", "#617449");
    drawFloorZone(0.6, 1.1, 5.2, 2.5, "rgba(240,166,90,0.07)", "#7f5c3e");
    drawFloorZone(27.3, 18.2, 4.0, 2.8, "rgba(104,199,255,0.07)", "#456c7b");
  }

  function drawFloorZone(x, y, width, depth, fill, stroke) {
    const points = [project(x, y), project(x + width, y), project(x + width, y + depth), project(x, y + depth)];
    ctx.save();
    ctx.setLineDash([3, 3]);
    pathPolygon(points, fill, stroke, 0.8);
    ctx.restore();
  }

  function drawHeatmap() {
    if (!state.heat) return;
    const hotspots = [
      { x: 4.8, y: 19.4, size: 65, color: "rgba(255,107,100,0.22)" },
      { x: 6.0, y: 11.8, size: 53, color: "rgba(240,166,90,0.18)" },
      { x: 18.3, y: 11.5, size: 58, color: "rgba(240,166,90,0.17)" },
      { x: 26.8, y: 6.0, size: 48, color: "rgba(104,199,255,0.14)" },
    ];
    ctx.save();
    ctx.globalCompositeOperation = "screen";
    hotspots.forEach((spot) => {
      const point = project(spot.x, spot.y);
      const radius = spot.size * state.zoom;
      const gradient = ctx.createRadialGradient(point.x, point.y, 0, point.x, point.y, radius);
      gradient.addColorStop(0, spot.color);
      gradient.addColorStop(0.45, spot.color.replace(/0\.\d+\)/, "0.08)"));
      gradient.addColorStop(1, "rgba(0,0,0,0)");
      ctx.fillStyle = gradient;
      ctx.fillRect(point.x - radius, point.y - radius, radius * 2, radius * 2);
    });
    ctx.restore();
  }

  function drawPrism(x, y, width, depth, height, palette, stroke = "rgba(255,255,255,0.12)", baseZ = 0) {
    const b1 = project(x, y, baseZ);
    const b2 = project(x + width, y, baseZ);
    const b3 = project(x + width, y + depth, baseZ);
    const b4 = project(x, y + depth, baseZ);
    const t1 = project(x, y, baseZ + height);
    const t2 = project(x + width, y, baseZ + height);
    const t3 = project(x + width, y + depth, baseZ + height);
    const t4 = project(x, y + depth, baseZ + height);

    if (state.view === "top") {
      pathPolygon([b1, b2, b3, b4], palette.top, stroke, 0.65);
      return;
    }

    const sideFaces = [
      { points: [b1, b4, t4, t1], fill: palette.right, depth: cameraDepth(x, y + depth / 2) },
      { points: [b2, b3, t3, t2], fill: palette.right, depth: cameraDepth(x + width, y + depth / 2) },
      { points: [b1, b2, t2, t1], fill: palette.left, depth: cameraDepth(x + width / 2, y) },
      { points: [b4, b3, t3, t4], fill: palette.left, depth: cameraDepth(x + width / 2, y + depth) },
    ];
    sideFaces.sort((a, b) => a.depth - b.depth).forEach((face) => {
      pathPolygon(face.points, face.fill, stroke, 0.55);
    });
    pathPolygon([t1, t2, t3, t4], palette.top, stroke, 0.65);
  }

  function drawRack(rack) {
    if (state.view === "top") {
      const p1 = project(rack.x, rack.y);
      const p2 = project(rack.x + rack.width, rack.y + rack.depth);
      ctx.save();
      ctx.fillStyle = "#26342f";
      ctx.strokeStyle = "#60716a";
      ctx.lineWidth = 0.8;
      ctx.fillRect(p1.x, p1.y, p2.x - p1.x, p2.y - p1.y);
      ctx.strokeRect(p1.x, p1.y, p2.x - p1.x, p2.y - p1.y);
      for (let bay = 1; bay < SHELF_COLUMNS; bay += 1) {
        const bx = p1.x + ((p2.x - p1.x) * bay) / SHELF_COLUMNS;
        ctx.beginPath();
        ctx.moveTo(bx, p1.y);
        ctx.lineTo(bx, p2.y);
        ctx.stroke();
      }
      for (let depthRow = 1; depthRow < SHELF_DEPTH_ROWS; depthRow += 1) {
        const rowY = p1.y + ((p2.y - p1.y) * depthRow) / SHELF_DEPTH_ROWS;
        ctx.beginPath();
        ctx.moveTo(p1.x, rowY);
        ctx.lineTo(p2.x, rowY);
        ctx.stroke();
      }
      rack.slots.forEach((slot) => {
        if (state.level !== "all" && Number(state.level) !== slot.level) return;
        const isTarget = slot.reservedBy === state.selectedId;
        const isFresh = slot.placedAt !== null && state.elapsed - slot.placedAt < 12;
        if (!slot.occupied && !isTarget) return;
        const bayWidth = (p2.x - p1.x) / SHELF_COLUMNS;
        const rowHeight = (p2.y - p1.y) / SHELF_DEPTH_ROWS;
        const inset = 2;
        const x = p1.x + slot.bay * bayWidth + inset;
        const y = p1.y + (slot.depthRow || 0) * rowHeight + inset;
        const width = Math.max(2, bayWidth - inset * 2);
        const height = Math.max(2, rowHeight - inset * 2);
        ctx.globalAlpha = state.level === "all" ? 0.35 + slot.level * 0.14 : 0.92;
        ctx.fillStyle = slot.occupied
          ? (isFresh ? ACTIVE_BOX_PALETTE.top : BOX_PALETTES[slot.colorIndex].top)
          : "rgba(201,244,108,0.08)";
        ctx.fillRect(x, y, width, height);
        if (isTarget || isFresh) {
          ctx.globalAlpha = 1;
          ctx.strokeStyle = isFresh ? COLORS.lime : robotTypeFor(slot.reservedBy || 1).accent;
          ctx.lineWidth = 1.3;
          ctx.strokeRect(x, y, width, height);
        }
      });
      ctx.restore();
      return;
    }

    drawPrism(rack.x, rack.y, rack.width, rack.depth, RACK_HEIGHT, {
      top: "rgba(65,82,76,0.30)",
      left: "rgba(37,49,45,0.28)",
      right: "rgba(46,60,55,0.30)",
    }, "rgba(102,122,114,0.34)");

    rack.slots.forEach((slot) => {
      if (!slot.occupied) return;
      if (state.level !== "all" && Number(state.level) !== slot.level) return;
      const geometry = slotGeometry(rack, slot);
      const isFresh = slot.placedAt !== null && state.elapsed - slot.placedAt < 12;
      drawPrism(
        geometry.x,
        geometry.y,
        geometry.width,
        geometry.depth,
        geometry.height,
        isFresh ? ACTIVE_BOX_PALETTE : BOX_PALETTES[slot.colorIndex],
        isFresh ? COLORS.lime : "rgba(255,255,255,0.1)",
        geometry.baseZ,
      );
    });

    ctx.save();
    ctx.strokeStyle = state.level === "all" ? "#63766e" : COLORS.lime;
    ctx.lineWidth = 1;
    [0, rack.width].forEach((dx) => {
      [0, rack.depth].forEach((dy) => {
        const bottom = project(rack.x + dx, rack.y + dy, 0);
        const top = project(rack.x + dx, rack.y + dy, RACK_HEIGHT);
        ctx.beginPath();
        ctx.moveTo(bottom.x, bottom.y);
        ctx.lineTo(top.x, top.y);
        ctx.stroke();
      });
    });
    ctx.globalAlpha = state.level === "all" ? 0.32 : 0.18;
    ctx.lineWidth = 0.45;
    for (let bay = 1; bay < SHELF_COLUMNS; bay += 1) {
      const x = rack.x + (rack.width * bay) / SHELF_COLUMNS;
      const bottom = project(x, rack.y + rack.depth, 0);
      const top = project(x, rack.y + rack.depth, RACK_HEIGHT);
      ctx.beginPath();
      ctx.moveTo(bottom.x, bottom.y);
      ctx.lineTo(top.x, top.y);
      ctx.stroke();
    }
    for (let level = 0; level <= 3; level += 1) {
      const z = 0.03 + level * SHELF_GAP;
      const points = [
        project(rack.x, rack.y, z),
        project(rack.x + rack.width, rack.y, z),
        project(rack.x + rack.width, rack.y + rack.depth, z),
        project(rack.x, rack.y + rack.depth, z),
      ];
      ctx.globalAlpha = state.level === "all" || Number(state.level) === level ? 0.85 : 0.22;
      pathPolygon(points, null, "#60736b", 0.75);
      const rowGuideStart = project(rack.x, rack.y + rack.depth / 2, z);
      const rowGuideEnd = project(rack.x + rack.width, rack.y + rack.depth / 2, z);
      ctx.globalAlpha = state.level === "all" || Number(state.level) === level ? 0.38 : 0.1;
      ctx.beginPath();
      ctx.moveTo(rowGuideStart.x, rowGuideStart.y);
      ctx.lineTo(rowGuideEnd.x, rowGuideEnd.y);
      ctx.stroke();
    }
    const labelPoint = project(rack.x + rack.width / 2, rack.y + rack.depth, RACK_HEIGHT + 0.18);
    ctx.globalAlpha = 0.72;
    ctx.fillStyle = "#aebbb6";
    ctx.font = `${Math.max(6, 7 * state.zoom)}px ui-monospace, monospace`;
    ctx.textAlign = "center";
    ctx.fillText(`${rack.zone}-${pad(rack.id)}`, labelPoint.x, labelPoint.y);
    ctx.restore();

    const selectedSlot = rack.slots.find((slot) => slot.reservedBy === state.selectedId);
    if (selectedSlot) {
      ctx.save();
      ctx.font = `${Math.max(6, 6.5 * state.zoom)}px ui-monospace, monospace`;
      ctx.textAlign = "left";
      [0, 1].forEach((depthRow) => {
        const rowGeometry = slotGeometry(rack, { ...selectedSlot, depthRow });
        const marker = project(rack.x + rack.width + 0.06, rowGeometry.y + rowGeometry.depth / 2, rowGeometry.baseZ + 0.03);
        ctx.fillStyle = depthRow === 0 ? "rgba(104,199,255,0.78)" : "rgba(201,244,108,0.78)";
        ctx.fillText(shelfRowName(depthRow), marker.x, marker.y);
      });
      ctx.restore();
    }

    rack.slots.forEach((slot) => {
      if (state.level !== "all" && Number(state.level) !== slot.level) return;
      const isSelectedTarget = slot.reservedBy === state.selectedId;
      const isFresh = slot.placedAt !== null && state.elapsed - slot.placedAt < 12;
      if (!isSelectedTarget && !isFresh) return;
      drawSlotHighlight(rack, slot, isFresh);
    });
  }

  function drawSlotHighlight(rack, slot, fresh) {
    const geometry = slotGeometry(rack, slot);
    const frontY = rack.y + rack.depth + 0.02;
    const points = [
      project(geometry.x, frontY, geometry.baseZ),
      project(geometry.x + geometry.width, frontY, geometry.baseZ),
      project(geometry.x + geometry.width, frontY, geometry.baseZ + geometry.height + 0.05),
      project(geometry.x, frontY, geometry.baseZ + geometry.height + 0.05),
    ];
    const reservedType = robotTypeFor(slot.reservedBy || 1);
    ctx.save();
    pathPolygon(points, fresh ? "rgba(201,244,108,0.08)" : "rgba(104,199,255,0.055)", fresh ? COLORS.lime : reservedType.accent, 1.2);
    const anchor = points[3];
    const label = fresh ? `${slot.cargoId} ✓` : `目标 C${slot.bay + 1} / ${shelfRowName(slot.depthRow)} / L${slot.level}`;
    ctx.font = `${Math.max(6, 7 * state.zoom)}px ui-monospace, monospace`;
    const width = ctx.measureText(label).width + 8;
    ctx.fillStyle = "rgba(8,13,12,0.92)";
    ctx.fillRect(anchor.x - 2, anchor.y - 13, width, 12);
    ctx.fillStyle = fresh ? COLORS.lime : reservedType.accent;
    ctx.textAlign = "left";
    ctx.fillText(label, anchor.x + 2, anchor.y - 4);
    ctx.restore();
  }

  function placementPose(robot) {
    if (robot.stage !== "placing" || !robot.slot) return null;
    const geometry = slotGeometry(robot.rack, robot.slot);
    const progress = clamp(robot.actionTimer / Math.max(0.1, robot.actionDuration), 0, 1);
    const frontY = robot.rack.y + robot.rack.depth + 0.1;
    const startX = robot.x - geometry.width / 2;
    const startY = robot.y - 0.22;
    let boxX = startX;
    let boxY = startY;
    let boxZ = 0.7;

    if (progress < 0.2) {
      const amount = progress / 0.2;
      boxX = lerp(startX, geometry.x, amount);
      boxY = lerp(startY, frontY + 0.18, amount);
    } else if (progress < 0.72) {
      const amount = (progress - 0.2) / 0.52;
      boxX = geometry.x;
      boxY = lerp(frontY + 0.18, frontY, amount);
      boxZ = lerp(0.7, geometry.baseZ, amount);
    } else if (progress < 0.92) {
      const amount = (progress - 0.72) / 0.2;
      boxX = geometry.x;
      boxY = lerp(frontY, geometry.y, amount);
      boxZ = geometry.baseZ;
    } else {
      boxX = geometry.x;
      boxY = geometry.y;
      boxZ = geometry.baseZ;
    }

    return { geometry, progress, boxX, boxY, boxZ };
  }

  function drawPlacementCargo(robot) {
    const pose = placementPose(robot);
    if (!pose) return;
    const { geometry, progress, boxX, boxY, boxZ } = pose;

    ctx.save();
    drawPrism(boxX, boxY, geometry.width, geometry.depth, geometry.height, ACTIVE_BOX_PALETTE, robot.type.accent, boxZ);
    if (robot.id === state.selectedId) {
      const anchor = project(boxX + geometry.width / 2, boxY + geometry.depth / 2, boxZ + geometry.height + 0.18);
      const extensionEnvelope = progress < 0.2 ? progress / 0.2 : progress < 0.92 ? 1 : (1 - progress) / 0.08;
      const label = `${robot.type.key} 伸臂 ${Math.round(robot.type.extension * clamp(extensionEnvelope, 0, 1) * 100)}cm · L${robot.level}`;
      ctx.font = "7px ui-monospace, monospace";
      const width = ctx.measureText(label).width + 8;
      ctx.fillStyle = "rgba(8,13,12,0.9)";
      ctx.fillRect(anchor.x - width / 2, anchor.y - 10, width, 12);
      ctx.fillStyle = robot.type.accent;
      ctx.textAlign = "center";
      ctx.fillText(label, anchor.x, anchor.y - 2);
    }
    ctx.restore();
  }

  function drawTelescopicArms(robot, point, scale, topView = false) {
    const pose = placementPose(robot);
    if (!pose) return;
    const { geometry, progress, boxX, boxY, boxZ } = pose;
    const cargoZ = boxZ + geometry.height * (topView ? 0 : 0.52);
    const gripWorld = [
      { x: boxX + geometry.width * 0.16, y: boxY + geometry.depth * 0.5, z: cargoZ },
      { x: boxX + geometry.width * 0.84, y: boxY + geometry.depth * 0.5, z: cargoZ },
    ];
    const rawGrips = gripWorld.map((grip) => project(grip.x, grip.y, grip.z));
    const s = topView ? scale : 0.78 * scale;
    const shoulders = topView
      ? [
          { x: point.x - 2.5 * s, y: point.y - 1.4 * s },
          { x: point.x + 2.5 * s, y: point.y - 1.4 * s },
        ]
      : [
          { x: point.x - 5 * s, y: point.y - 25 * s },
          { x: point.x + 5 * s, y: point.y - 25 * s },
        ];
    const standby = topView
      ? [
          { x: point.x - 7 * s, y: point.y - 5 * s },
          { x: point.x + 7 * s, y: point.y - 5 * s },
        ]
      : [
          { x: point.x - 8 * s, y: point.y - 15 * s },
          { x: point.x + 8 * s, y: point.y - 15 * s },
        ];
    const retract = clamp((progress - 0.92) / 0.08, 0, 1);
    const sleeveRatio = clamp(0.68 - robot.type.extension * 0.24, 0.42, 0.64);

    ctx.save();
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    shoulders.forEach((shoulder, index) => {
      const side = index === 0 ? -1 : 1;
      const grip = {
        x: lerp(rawGrips[index].x, standby[index].x, retract),
        y: lerp(rawGrips[index].y, standby[index].y, retract),
      };
      const dx = grip.x - shoulder.x;
      const dy = grip.y - shoulder.y;
      const length = Math.max(1, Math.hypot(dx, dy));
      const perpendicularX = -dy / length;
      const perpendicularY = dx / length;
      const elbow = {
        x: shoulder.x + dx * sleeveRatio + perpendicularX * side * 2.2 * s * (1 - retract),
        y: shoulder.y + dy * sleeveRatio + perpendicularY * side * 2.2 * s * (1 - retract),
      };

      ctx.strokeStyle = "rgba(7,13,12,0.94)";
      ctx.lineWidth = 5.4 * s;
      ctx.beginPath();
      ctx.moveTo(shoulder.x, shoulder.y);
      ctx.lineTo(elbow.x, elbow.y);
      ctx.stroke();

      ctx.strokeStyle = "#879690";
      ctx.lineWidth = 3.4 * s;
      ctx.beginPath();
      ctx.moveTo(shoulder.x, shoulder.y);
      ctx.lineTo(elbow.x, elbow.y);
      ctx.stroke();

      ctx.strokeStyle = "rgba(7,13,12,0.96)";
      ctx.lineWidth = 4.3 * s;
      ctx.beginPath();
      ctx.moveTo(elbow.x, elbow.y);
      ctx.lineTo(grip.x, grip.y);
      ctx.stroke();

      ctx.strokeStyle = robot.type.accent;
      ctx.lineWidth = 2.15 * s;
      ctx.beginPath();
      ctx.moveTo(elbow.x, elbow.y);
      ctx.lineTo(grip.x, grip.y);
      ctx.stroke();

      ctx.beginPath();
      ctx.arc(shoulder.x, shoulder.y, 2.2 * s, 0, Math.PI * 2);
      ctx.fillStyle = "#d5dfdb";
      ctx.fill();
      ctx.strokeStyle = "#23302c";
      ctx.lineWidth = 0.9 * s;
      ctx.stroke();

      ctx.beginPath();
      ctx.arc(elbow.x, elbow.y, 2.65 * s, 0, Math.PI * 2);
      ctx.fillStyle = "#17221f";
      ctx.fill();
      ctx.beginPath();
      ctx.arc(elbow.x, elbow.y, 1.45 * s, 0, Math.PI * 2);
      ctx.fillStyle = robot.type.accent;
      ctx.fill();

      const unitX = dx / length;
      const unitY = dy / length;
      ctx.strokeStyle = "#d9e3df";
      ctx.lineWidth = 1.25 * s;
      ctx.beginPath();
      ctx.moveTo(grip.x - unitX * 2.4 * s + perpendicularX * 2.3 * s, grip.y - unitY * 2.4 * s + perpendicularY * 2.3 * s);
      ctx.lineTo(grip.x, grip.y);
      ctx.lineTo(grip.x - unitX * 2.4 * s - perpendicularX * 2.3 * s, grip.y - unitY * 2.4 * s - perpendicularY * 2.3 * s);
      ctx.stroke();
    });
    ctx.restore();
  }

  function drawRoutes() {
    if (!state.routes) return;
    const selected = robots.find((robot) => robot.id === state.selectedId);
    const candidates = robots.filter((robot) => ["toPick", "toRack", "returning"].includes(robot.stage));
    const routeRobots = [...candidates.filter((robot) => robot.id % 13 === 0), selected].filter(Boolean);
    const unique = [...new Map(routeRobots.map((robot) => [robot.id, robot])).values()];

    unique.forEach((robot) => {
      if (!robot.path || robot.pathIndex >= robot.path.length) return;
      const points = [{ x: robot.x, y: robot.y }, ...robot.path.slice(robot.pathIndex)].map((point) => project(point.x, point.y, 0.03));
      ctx.save();
      ctx.beginPath();
      ctx.moveTo(points[0].x, points[0].y);
      points.slice(1).forEach((point) => ctx.lineTo(point.x, point.y));
      ctx.strokeStyle = robot.id === state.selectedId ? COLORS.lime : "rgba(104,199,255,0.20)";
      ctx.lineWidth = robot.id === state.selectedId ? 1.25 : 0.65;
      ctx.setLineDash(robot.id === state.selectedId ? [5, 4] : [2, 5]);
      ctx.stroke();
      const end = points[points.length - 1];
      ctx.setLineDash([]);
      ctx.beginPath();
      ctx.arc(end.x, end.y, robot.id === state.selectedId ? 3.2 : 2, 0, Math.PI * 2);
      ctx.fillStyle = robot.id === state.selectedId ? COLORS.lime : "rgba(104,199,255,0.45)";
      ctx.fill();
      ctx.restore();
    });
  }

  function roundedRect(context, x, y, width, height, radius) {
    const r = Math.min(radius, Math.abs(width) / 2, Math.abs(height) / 2);
    context.beginPath();
    context.moveTo(x + r, y);
    context.arcTo(x + width, y, x + width, y + height, r);
    context.arcTo(x + width, y + height, x, y + height, r);
    context.arcTo(x, y + height, x, y, r);
    context.arcTo(x, y, x + width, y, r);
    context.closePath();
  }

  function drawRobot(robot) {
    const point = project(robot.x, robot.y, 0.02);
    const category = categoryFor(robot);
    const filterMatch = state.filter === "all" || state.filter === category;
    const selected = robot.id === state.selectedId;
    const color = category === "moving" ? COLORS.lime : category === "handling" ? COLORS.blue : category === "warning" ? COLORS.red : COLORS.idle;
    const scale = state.view === "top" ? clamp(projectionMetrics().scale / 17, 0.58, 1.1) : clamp(state.zoom, 0.63, 1.25);
    const walk = ["toPick", "toRack", "returning"].includes(robot.stage) ? Math.sin(robot.phase) : 0;
    const actionProgress = robot.actionDuration ? clamp(robot.actionTimer / robot.actionDuration, 0, 1) : 0;
    const lift = robot.stage === "picking" ? Math.sin(actionProgress * Math.PI) * 0.65 : 0;
    const showHeldBox = (robot.carrying && robot.stage !== "placing") || robot.stage === "picking";

    ctx.save();
    ctx.globalAlpha = filterMatch ? 1 : 0.09;

    if (state.view === "top") {
      ctx.beginPath();
      ctx.ellipse(point.x, point.y, 6.7 * scale, 4.4 * scale, robot.heading, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(0,0,0,0.36)";
      ctx.fill();
      ctx.beginPath();
      ctx.arc(point.x, point.y - 1.5 * scale, (selected ? 5.2 : 4.1) * scale, 0, Math.PI * 2);
      ctx.fillStyle = selected ? "#e7f6be" : "#d2dcd7";
      ctx.fill();
      ctx.strokeStyle = color;
      ctx.lineWidth = selected ? 2.2 : 1.2;
      ctx.stroke();
      ctx.beginPath();
      ctx.arc(point.x, point.y - 1.5 * scale, 1.7 * scale, 0, Math.PI * 2);
      ctx.fillStyle = robot.type.accent;
      ctx.fill();
      const hx = Math.cos(robot.heading) * 6 * scale;
      const hy = Math.sin(robot.heading) * 6 * scale;
      ctx.beginPath();
      ctx.moveTo(point.x, point.y);
      ctx.lineTo(point.x + hx, point.y + hy);
      ctx.stroke();
      if (robot.stage === "placing") drawTelescopicArms(robot, point, scale, true);
    } else {
      const s = 0.78 * scale;
      ctx.beginPath();
      ctx.ellipse(point.x, point.y + 2 * s, 8.5 * s, 3.2 * s, 0, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(0,0,0,0.43)";
      ctx.fill();

      const legSwing = walk * 2.7 * s;
      const footY = point.y - 1 * s;
      const hipY = point.y - 13 * s;
      ctx.lineCap = "round";
      ctx.lineWidth = 2.2 * s;
      ctx.strokeStyle = "#87948f";
      ctx.beginPath();
      ctx.moveTo(point.x - 2.2 * s, hipY);
      ctx.lineTo(point.x - 3.2 * s - legSwing, footY);
      ctx.moveTo(point.x + 2.2 * s, hipY);
      ctx.lineTo(point.x + 3.2 * s + legSwing, footY);
      ctx.stroke();

      roundedRect(ctx, point.x - 5 * s, point.y - 30 * s, 10 * s, 18 * s, 3 * s);
      ctx.fillStyle = "#c5cfcb";
      ctx.fill();
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.1 * s;
      ctx.stroke();
      ctx.fillStyle = robot.type.accent;
      ctx.fillRect(point.x - 4 * s, point.y - 22.5 * s, 8 * s, 1.8 * s);

      const armY = point.y - 25 * s;
      const armRaise = lift * 4 * s;
      if (robot.stage === "placing") {
        drawTelescopicArms(robot, point, scale);
      } else {
        ctx.strokeStyle = "#96a39e";
        ctx.lineWidth = 2.1 * s;
        ctx.beginPath();
        if (robot.carrying || robot.stage === "picking") {
          ctx.moveTo(point.x - 5 * s, armY);
          ctx.lineTo(point.x - 8 * s, armY + 6 * s - armRaise * 0.45);
          ctx.lineTo(point.x - 4 * s, armY + 9 * s - armRaise);
          ctx.moveTo(point.x + 5 * s, armY);
          ctx.lineTo(point.x + 8 * s, armY + 6 * s - armRaise * 0.45);
          ctx.lineTo(point.x + 4 * s, armY + 9 * s - armRaise);
        } else {
          ctx.moveTo(point.x - 5 * s, armY);
          ctx.lineTo(point.x - 7 * s + walk * 1.5 * s, armY + 12 * s);
          ctx.moveTo(point.x + 5 * s, armY);
          ctx.lineTo(point.x + 7 * s - walk * 1.5 * s, armY + 12 * s);
        }
        ctx.stroke();
      }

      if (showHeldBox) {
        const boxY = point.y - 13 * s - armRaise;
        ctx.fillStyle = "#a87545";
        ctx.fillRect(point.x - 5 * s, boxY - 7 * s, 10 * s, 8 * s);
        ctx.strokeStyle = "#d1a66b";
        ctx.lineWidth = 0.7;
        ctx.strokeRect(point.x - 5 * s, boxY - 7 * s, 10 * s, 8 * s);
      }

      ctx.beginPath();
      ctx.arc(point.x, point.y - 35 * s, 4.2 * s, 0, Math.PI * 2);
      ctx.fillStyle = selected ? "#eff8da" : "#d9e2de";
      ctx.fill();
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.1 * s;
      ctx.stroke();
      ctx.fillStyle = color;
      ctx.fillRect(point.x + 1.5 * s, point.y - 36 * s, 2 * s, 1.2 * s);
    }

    if (selected) {
      const radiusX = state.view === "top" ? 10 * scale : 12 * scale;
      const radiusY = state.view === "top" ? 7 * scale : 4.8 * scale;
      ctx.beginPath();
      ctx.ellipse(point.x, point.y + 1, radiusX, radiusY, 0, 0, Math.PI * 2);
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.1;
      ctx.setLineDash([3, 2]);
      ctx.stroke();
      ctx.setLineDash([]);

      const labelY = state.view === "top" ? point.y - 15 : point.y - 42 * scale;
      ctx.font = "8px ui-monospace, monospace";
      ctx.textAlign = "center";
      const label = `${robotCode(robot.id)} / ${robot.type.key} · ${stageLabel(robot)}`;
      const width = ctx.measureText(label).width + 10;
      ctx.fillStyle = "rgba(8,13,12,0.92)";
      ctx.fillRect(point.x - width / 2, labelY - 10, width, 15);
      ctx.strokeStyle = "rgba(201,244,108,0.45)";
      ctx.strokeRect(point.x - width / 2, labelY - 10, width, 15);
      ctx.fillStyle = "#dbe5e1";
      ctx.fillText(label, point.x, labelY);
    }
    ctx.restore();
  }

  function drawScene() {
    ctx.clearRect(0, 0, renderWidth, renderHeight);
    drawFloor();
    drawHeatmap();
    drawRoutes();

    const drawables = [
      ...racks.map((rack) => ({ depth: cameraDepth(rack.x + rack.width / 2, rack.y + rack.depth / 2), draw: () => drawRack(rack) })),
      ...robots
        .filter((robot) => robot.stage === "placing")
        .map((robot) => ({ depth: cameraDepth(robot.x, robot.y) + 0.02, draw: () => drawPlacementCargo(robot) })),
      ...robots.map((robot) => ({ depth: cameraDepth(robot.x, robot.y) + 0.04, draw: () => drawRobot(robot) })),
    ];

    drawables.sort((a, b) => a.depth - b.depth).forEach((item) => item.draw());
    drawWorldLabels();
    updateZoneLabels();
  }

  function updateZoneLabels() {
    [
      [".inbound-label", 3.2, 19.9],
      [".outbound-label", 3.2, 2.35],
      [".charge-label", 29.3, 19.6],
    ].forEach(([selector, x, y]) => {
      const label = $(selector);
      const point = project(x, y, 0.03);
      label.style.left = `${point.x}px`;
      label.style.top = `${point.y}px`;
      label.style.right = "auto";
      label.style.bottom = "auto";
      label.style.transform = "translate(-50%, -50%)";
    });
  }

  function drawWorldLabels() {
    ctx.save();
    ctx.fillStyle = "rgba(157,171,165,0.55)";
    ctx.font = "7px ui-monospace, monospace";
    ctx.textAlign = "center";
    for (let x = 5; x < WORLD.width; x += 5) {
      const point = project(x, WORLD.height + 0.65);
      ctx.fillText(pad(x), point.x, point.y);
    }
    ctx.restore();
  }

  function drawPortrait(robot) {
    const width = portraitCanvas.width;
    const height = portraitCanvas.height;
    portraitCtx.clearRect(0, 0, width, height);
    portraitCtx.save();
    const color = categoryFor(robot) === "warning" ? COLORS.red : categoryFor(robot) === "handling" ? COLORS.blue : categoryFor(robot) === "idle" ? COLORS.idle : COLORS.lime;
    portraitCtx.strokeStyle = "rgba(70,85,79,0.35)";
    portraitCtx.lineWidth = 1;
    for (let y = 15; y < height; y += 18) {
      portraitCtx.beginPath();
      portraitCtx.moveTo(12, y);
      portraitCtx.lineTo(width - 12, y);
      portraitCtx.stroke();
    }
    portraitCtx.translate(width / 2 - 10, 15);
    const walk = ["toPick", "toRack", "returning"].includes(robot.stage) ? Math.sin(robot.phase) * 4 : 0;
    const actionProgress = clamp(robot.actionTimer / Math.max(0.1, robot.actionDuration), 0, 1);
    const portraitReach = robot.stage === "placing"
      ? (actionProgress < 0.2 ? actionProgress / 0.2 : actionProgress < 0.92 ? 1 : (1 - actionProgress) / 0.08)
      : 0;
    const lift = robot.stage === "picking" ? Math.sin(actionProgress * Math.PI) * 14 : 0;
    const portraitHasBox = (robot.carrying && robot.stage !== "placing") || robot.stage === "picking";
    portraitCtx.lineCap = "round";
    portraitCtx.strokeStyle = "#8c9994";
    portraitCtx.lineWidth = 5;
    portraitCtx.beginPath();
    portraitCtx.moveTo(-7, 80);
    portraitCtx.lineTo(-10 - walk, 113);
    portraitCtx.moveTo(7, 80);
    portraitCtx.lineTo(10 + walk, 113);
    portraitCtx.stroke();
    roundedRect(portraitCtx, -14, 35, 28, 48, 7);
    portraitCtx.fillStyle = "#cbd5d1";
    portraitCtx.fill();
    portraitCtx.strokeStyle = color;
    portraitCtx.lineWidth = 2;
    portraitCtx.stroke();
    portraitCtx.fillStyle = robot.type.accent;
    portraitCtx.fillRect(-11, 55, 22, 4);
    if (robot.stage === "placing") {
      const retract = clamp((actionProgress - 0.92) / 0.08, 0, 1);
      const targetY = lerp(66 - portraitReach * (14 + (robot.level - 1) * 11 + robot.type.extension * 12), 69, retract);
      const grips = [{ x: -11, y: targetY }, { x: 11, y: targetY }];
      const shoulders = [{ x: -14, y: 46 }, { x: 14, y: 46 }];
      shoulders.forEach((shoulder, index) => {
        const side = index === 0 ? -1 : 1;
        const grip = grips[index];
        const elbow = {
          x: lerp(shoulder.x, grip.x, 0.52) + side * (7 + robot.type.extension * 4),
          y: lerp(shoulder.y, grip.y, 0.52),
        };
        portraitCtx.strokeStyle = "#17211e";
        portraitCtx.lineWidth = 8;
        portraitCtx.beginPath();
        portraitCtx.moveTo(shoulder.x, shoulder.y);
        portraitCtx.lineTo(elbow.x, elbow.y);
        portraitCtx.stroke();
        portraitCtx.strokeStyle = "#8b9994";
        portraitCtx.lineWidth = 5;
        portraitCtx.beginPath();
        portraitCtx.moveTo(shoulder.x, shoulder.y);
        portraitCtx.lineTo(elbow.x, elbow.y);
        portraitCtx.stroke();
        portraitCtx.strokeStyle = "#17211e";
        portraitCtx.lineWidth = 7;
        portraitCtx.beginPath();
        portraitCtx.moveTo(elbow.x, elbow.y);
        portraitCtx.lineTo(grip.x, grip.y);
        portraitCtx.stroke();
        portraitCtx.strokeStyle = robot.type.accent;
        portraitCtx.lineWidth = 3;
        portraitCtx.beginPath();
        portraitCtx.moveTo(elbow.x, elbow.y);
        portraitCtx.lineTo(grip.x, grip.y);
        portraitCtx.stroke();
        portraitCtx.beginPath();
        portraitCtx.arc(elbow.x, elbow.y, 4, 0, Math.PI * 2);
        portraitCtx.fillStyle = robot.type.accent;
        portraitCtx.fill();
        portraitCtx.strokeStyle = "#17211e";
        portraitCtx.lineWidth = 1.5;
        portraitCtx.stroke();
        portraitCtx.strokeStyle = "#d9e3df";
        portraitCtx.lineWidth = 2;
        portraitCtx.beginPath();
        portraitCtx.moveTo(grip.x + side * 5, grip.y - 3);
        portraitCtx.lineTo(grip.x, grip.y);
        portraitCtx.lineTo(grip.x + side * 5, grip.y + 3);
        portraitCtx.stroke();
      });
    } else {
      portraitCtx.strokeStyle = "#93a09b";
      portraitCtx.lineWidth = 5;
      portraitCtx.beginPath();
      if (robot.carrying || robot.stage === "picking") {
        portraitCtx.moveTo(-14, 46);
        portraitCtx.lineTo(-23, 66 - lift * 0.45);
        portraitCtx.lineTo(-10, 69 - lift);
        portraitCtx.moveTo(14, 46);
        portraitCtx.lineTo(23, 66 - lift * 0.45);
        portraitCtx.lineTo(10, 69 - lift);
      } else {
        portraitCtx.moveTo(-14, 46);
        portraitCtx.lineTo(-21 + walk, 76);
        portraitCtx.moveTo(14, 46);
        portraitCtx.lineTo(21 - walk, 76);
      }
      portraitCtx.stroke();
    }
    if (portraitHasBox) {
      portraitCtx.fillStyle = "#a87545";
      portraitCtx.fillRect(-14, 59 - lift, 28, 22);
      portraitCtx.strokeStyle = "#d5a56e";
      portraitCtx.lineWidth = 1;
      portraitCtx.strokeRect(-14, 59 - lift, 28, 22);
    }
    portraitCtx.beginPath();
    portraitCtx.arc(0, 23, 11, 0, Math.PI * 2);
    portraitCtx.fillStyle = "#e4ece8";
    portraitCtx.fill();
    portraitCtx.strokeStyle = color;
    portraitCtx.lineWidth = 2;
    portraitCtx.stroke();
    portraitCtx.fillStyle = color;
    portraitCtx.fillRect(3, 20, 5, 2);
    portraitCtx.restore();

    portraitCtx.fillStyle = "rgba(201,244,108,0.7)";
    portraitCtx.font = "8px ui-monospace, monospace";
    portraitCtx.fillText(`${robotCode(robot.id)} / ${robot.type.key}`, 8, height - 7);
  }

  function stageProgress(robot) {
    if (["picking", "placing"].includes(robot.stage)) {
      return clamp((robot.actionTimer / robot.actionDuration) * 100, 0, 100);
    }
    if (["toPick", "toRack", "returning"].includes(robot.stage)) {
      return clamp((robot.distanceTravelled / Math.max(0.01, robot.stageDistance)) * 100, 0, 100);
    }
    if (robot.stage === "charging") return robot.battery;
    return robot.stage === "warning" ? 24 : 100;
  }

  function updateSelectedRobot() {
    const robot = robots.find((item) => item.id === state.selectedId) || robots[0];
    if (!robot) return;
    const category = categoryFor(robot);
    const status = $("#selectedStatus");
    const progress = Math.round(stageProgress(robot));
    const rackLabel = `${robot.rack.zone}区 · 货架 ${pad(robot.rack.id)} · 第 ${robot.bay} 列 · ${shelfRowName(robot.depthRow)}`;
    const targetHeight = slotGeometry(robot.rack, robot.slot).centerHeight;

    $("#robotIdTag").textContent = robotCode(robot.id);
    $("#selectedRobotName").textContent = `${robot.type.name} · 单元 ${pad(robot.id, 3)}`;
    $("#selectedRobotPosition").textContent = `坐标 ${robot.x.toFixed(1)}, ${robot.y.toFixed(1)} · 通道 ${pad(Math.max(1, Math.round(robot.y / 2)), 2)}`;
    $("#selectedRobotType").textContent = robot.type.name;
    $("#selectedRobotType").style.color = robot.type.accent;
    $("#selectedReach").textContent = `最高 L${robot.type.maxLevel} · ${robot.type.maxHeight.toFixed(2)}m`;
    status.className = `state-pill ${category}`;
    status.innerHTML = `<i></i>${stageLabel(robot)}`;
    $("#batteryValue").textContent = `${Math.round(robot.battery)}%`;
    $("#batteryBar").style.width = `${robot.battery}%`;
    $("#batteryBar").style.background = robot.battery < 18 ? COLORS.red : COLORS.lime;
    $("#selectedTaskId").textContent = `#${taskCode(robot)}`;
    $("#selectedTaskTitle").textContent = `将货物送至 ${rackLabel}`;
    $("#selectedCargo").textContent = `${cargoCode(robot)} · 小型箱 · ${robot.type.key} 伸臂放置`;
    $("#selectedLevel").textContent = `L${robot.level} · ${targetHeight.toFixed(2)}m`;
    $("#selectedAction").textContent = stageLabel(robot);
    $("#selectedProgressText").textContent = `${progress}%`;
    $("#selectedProgressBar").style.width = `${progress}%`;

    const steps = $$(".task-steps span");
    steps.forEach((step) => { step.className = ""; });
    if (["toPick", "picking"].includes(robot.stage)) {
      steps[0].classList.add("active");
    } else if (["toRack", "placing"].includes(robot.stage)) {
      steps[0].classList.add("done");
      steps[1].classList.add(robot.stage === "toRack" ? "active" : "done");
      if (robot.stage === "placing") steps[2].classList.add("active");
    } else if (["returning", "idle", "charging"].includes(robot.stage)) {
      steps.forEach((step) => step.classList.add("done"));
    }
    drawPortrait(robot);
  }

  function updateMetrics() {
    const counts = robots.reduce((result, robot) => {
      result[categoryFor(robot)] += 1;
      return result;
    }, { moving: 0, handling: 0, idle: 0, warning: 0 });
    const active = counts.moving + counts.handling;
    const waiting = backendAuthoritative ? 0 : Math.max(3, Math.round(state.flowRate * 0.31 - counts.idle * 0.35));
    const robotCount = Math.max(1, robots.length);
    const utilization = Math.round((active / robotCount) * 100);
    const movingPercent = (counts.moving / robotCount) * 100;
    const activePercent = (active / robotCount) * 100;

    $("#onlineCount").textContent = robots.length - robots.filter((robot) => robot.alert === "离线").length;
    $("#activeTaskCount").textContent = active;
    $("#waitingTaskCount").textContent = waiting;
    $("#taskLoadBar").style.width = `${Math.min(100, active)}%`;
    $("#completedCount").textContent = state.completed.toLocaleString("zh-CN");
    $("#warningCount").textContent = counts.warning;
    const lowBattery = robots.filter((robot) => robot.battery < 28).length;
    $("#warningDetail").textContent = `低电量 ${lowBattery} · 路径阻塞 ${counts.warning}`;
    $("#filterAll").textContent = robots.length;
    $("#filterMoving").textContent = counts.moving;
    $("#filterHandling").textContent = counts.handling;
    $("#filterIdle").textContent = counts.idle;
    $("#filterWarning").textContent = counts.warning;
    $("#legendMoving").textContent = counts.moving;
    $("#legendHandling").textContent = counts.handling;
    $("#legendIdle").textContent = counts.idle;
    $("#utilizationValue").textContent = `${utilization}%`;
    $("#utilizationDonut").querySelector("span").innerHTML = `${utilization}<small>%</small>`;
    $("#utilizationDonut").style.background = `conic-gradient(${COLORS.lime} 0 ${movingPercent}%, ${COLORS.blue} ${movingPercent}% ${activePercent}%, #303a37 ${activePercent}% 100%)`;
    $("#simClock").textContent = formatClock(state.elapsed);
    $("#connectionLatency").textContent = backendConnected && backendLatency !== null
      ? `${backendLatency} ms`
      : "离线";
    $("#fpsCounter").textContent = `${state.fps} FPS`;
    const shiftProgress = clamp((state.elapsed / (8 * 3600)) * 100, 0, 100);
    $("#timelineProgress").style.width = `${shiftProgress}%`;
    $("#timelineHandle").style.left = `${shiftProgress}%`;
  }

  function updateTaskQueue(force = false) {
    const queue = $("#taskQueue");
    if (!force && queue.dataset.second === String(Math.floor(state.elapsed / 3))) return;
    queue.dataset.second = String(Math.floor(state.elapsed / 3));
    const items = robots
      .filter((robot) => robot.stage === "toPick" || (!backendAuthoritative && robot.stage === "idle"))
      .sort((a, b) => (a.priority === "high" ? -1 : b.priority === "high" ? 1 : a.id - b.id))
      .slice(0, 4);
    queue.innerHTML = items.map((robot, index) => {
      const priority = robot.priority === "high" ? "high" : index === 1 ? "medium" : "normal";
      const eta = 2 + ((robot.id * 3 + index) % 8);
      return `
        <article class="queue-item">
          <i class="queue-priority ${priority}"></i>
          <div><h4>#${taskCode(robot)} · ${robotCode(robot.id)} · ${robot.type.key}</h4><p>入库台 → ${robot.rack.zone}-${pad(robot.rack.id)} / C${robot.bay} / ${shelfRowName(robot.depthRow)} / L${robot.level}</p></div>
          <time>${eta}m</time>
        </article>`;
    }).join("");
  }

  function renderLatestEvent() {
    const event = taskEvents[0] || { time: "--:--:--", robot: "SYSTEM", text: "暂无事件" };
    $("#eventTicker").innerHTML = `<span class="event-time">${event.time}</span><p><strong>${event.robot}</strong> ${event.text}</p>`;
  }

  function updateAllDom(force = false) {
    updateMetrics();
    updateSelectedRobot();
    updateTaskQueue(force);
  }

  function applyBackendSnapshot(snapshot) {
    const receivedAt = performance.now();
    const previousById = new Map(robots.map((robot) => [robot.id, robot]));
    if (backendSnapshotAt > 0) {
      backendInterpolationDuration = clamp(receivedAt - backendSnapshotAt, 80, 500);
    }
    backendSnapshotAt = receivedAt;
    const rackById = new Map();
    racks = snapshot.racks.map((rack) => {
      const hydrated = { ...rack, slots: rack.slots.map((slot) => ({ ...slot })) };
      rackById.set(hydrated.id, hydrated);
      return hydrated;
    });
    robots = snapshot.robots.map((payload) => {
      const previous = previousById.get(payload.id);
      const rack = rackById.get(payload.rackId) || racks[0];
      const slot = rack?.slots.find((item) => item.slotId === payload.slotId) || rack?.slots[0];
      return {
        ...payload,
        x: previous?.x ?? payload.x,
        y: previous?.y ?? payload.y,
        renderFromX: previous?.x ?? payload.x,
        renderFromY: previous?.y ?? payload.y,
        renderTargetX: payload.x,
        renderTargetY: payload.y,
        rack,
        slot,
      };
    });
    state.running = snapshot.running;
    state.speed = snapshot.speed;
    state.elapsed = snapshot.elapsed;
    state.completed = snapshot.completed;
    state.flowRate = snapshot.flowRate;
    if (!robots.some((robot) => robot.id === state.selectedId)) state.selectedId = robots[0]?.id || 1;
    backendLatency = Math.max(0, Date.now() - snapshot.generatedAtMs);
    backendConnected = true;
    backendAuthoritative = true;
    taskEvents.splice(0, taskEvents.length, ...snapshot.events.map((event) => ({
      time: event.time,
      robot: event.robot,
      text: event.text,
    })));
    $("#playIcon").textContent = state.running ? "Ⅱ" : "▶";
    $("#playButton").setAttribute("aria-label", state.running ? "暂停仿真" : "继续仿真");
    $$("#speedControl button").forEach((item) => item.classList.toggle("active", Number(item.dataset.speed) === state.speed));
    $("#flowRange").value = String(state.flowRate);
    const runtimeMode = $("#runtimeMode");
    if (runtimeMode) runtimeMode.textContent = `SIMULATION / ${snapshot.backend.toUpperCase()} · ${snapshot.runtime.workerProcessCount} PROCESSES`;
    updateAllDom(true);
    renderLatestEvent();
  }

  async function backendPost(path, payload = null) {
    const response = await fetch(path, {
      method: "POST",
      headers: payload ? { "Content-Type": "application/json" } : undefined,
      body: payload ? JSON.stringify(payload) : undefined,
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new Error(error.detail || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async function sendBackendControl(action, value = null) {
    try {
      const snapshot = await backendPost("/api/control", { action, value });
      applyBackendSnapshot(snapshot);
      return snapshot;
    } catch (error) {
      showToast(`后端操作失败：${error.message}`, "!");
      return null;
    }
  }

  function connectBackend() {
    if (window.location.protocol === "file:") return;
    clearTimeout(backendReconnectTimer);
    if (backendSocket && backendSocket.readyState < WebSocket.CLOSING) backendSocket.close();
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    backendSocket = new WebSocket(`${protocol}//${window.location.host}/ws/simulation`);
    backendSocket.addEventListener("message", (event) => {
      try {
        applyBackendSnapshot(JSON.parse(event.data));
      } catch (error) {
        console.error("Invalid backend snapshot", error);
      }
    });
    backendSocket.addEventListener("close", () => {
      backendConnected = false;
      backendLatency = null;
      const runtimeMode = $("#runtimeMode");
      if (runtimeMode) runtimeMode.textContent = "SIMULATION / OFFLINE DEMO";
      backendReconnectTimer = window.setTimeout(connectBackend, 2000);
    });
    backendSocket.addEventListener("error", () => backendSocket.close());
  }

  function animate(timestamp) {
    const rawDelta = Math.min(0.06, Math.max(0, (timestamp - lastTime) / 1000));
    lastTime = timestamp;
    const delta = state.running && !backendAuthoritative ? rawDelta * state.speed : 0;
    if (delta > 0) {
      state.elapsed += delta;
      robots.forEach((robot) => updateRobot(robot, delta));
      domAccumulator += rawDelta;
      eventAccumulator += delta;
    }
    if (backendConnected) domAccumulator += rawDelta;
    if (backendAuthoritative) {
      const interpolation = clamp((timestamp - backendSnapshotAt) / backendInterpolationDuration, 0, 1);
      robots.forEach((robot) => {
        robot.x = lerp(robot.renderFromX, robot.renderTargetX, interpolation);
        robot.y = lerp(robot.renderFromY, robot.renderTargetY, interpolation);
      });
    }

    if (domAccumulator > 0.25 || (!backendConnected && delta === 0)) {
      updateAllDom();
      domAccumulator = 0;
    }

    if (eventAccumulator > 7) {
      const robot = robots[Math.floor(Math.random() * robots.length)];
      if (robot.stage === "toRack") pushEvent(robot, `携带 ${cargoCode(robot)} 进入 ${robot.rack.zone} 区`);
      eventAccumulator = 0;
    }

    drawScene();
    frameCount += 1;
    if (timestamp - fpsTime >= 1000) {
      state.fps = Math.round((frameCount * 1000) / (timestamp - fpsTime));
      frameCount = 0;
      fpsTime = timestamp;
    }
    requestAnimationFrame(animate);
  }

  function showToast(message, icon = "✓") {
    clearTimeout(toastTimer);
    $("#toastText").textContent = message;
    $("#toastIcon").textContent = icon;
    $("#toast").classList.add("show");
    toastTimer = setTimeout(() => $("#toast").classList.remove("show"), 2300);
  }

  function resetView() {
    state.zoom = 1;
    state.panX = 0;
    state.panY = 0;
    state.cameraYaw = Math.PI / 4;
    state.cameraPitch = Math.PI / 6;
  }

  function changeZoom(amount, centerX = renderWidth / 2, centerY = renderHeight / 2) {
    const oldMetrics = projectionMetrics();
    const oldZoom = state.zoom;
    state.zoom = clamp(state.zoom * amount, 0.62, 1.9);
    const ratio = state.zoom / oldZoom;
    const newMetrics = projectionMetrics();
    state.panX += centerX - (newMetrics.originX + (centerX - oldMetrics.originX) * ratio);
    state.panY += centerY - (newMetrics.originY + (centerY - oldMetrics.originY) * ratio);
  }

  function selectNearestRobot(clientX, clientY) {
    const rect = canvas.getBoundingClientRect();
    const x = clientX - rect.left;
    const y = clientY - rect.top;
    let nearest = null;
    let nearestDistance = 22;
    robots.forEach((robot) => {
      const point = project(robot.x, robot.y);
      const distance = Math.hypot(point.x - x, point.y - y);
      if (distance < nearestDistance) {
        nearestDistance = distance;
        nearest = robot;
      }
    });
    if (nearest) {
      state.selectedId = nearest.id;
      updateSelectedRobot();
    }
  }

  function bindInteractions() {
    $("#playButton").addEventListener("click", () => {
      if (backendConnected) {
        const action = state.running ? "pause" : "resume";
        sendBackendControl(action).then((snapshot) => {
          if (snapshot) showToast(snapshot.running ? "仿真已继续" : "仿真已暂停", snapshot.running ? "▶" : "Ⅱ");
        });
        return;
      }
      if (backendAuthoritative) return showToast("后端连接中断，正在重连", "!");
      state.running = !state.running;
      $("#playIcon").textContent = state.running ? "Ⅱ" : "▶";
      $("#playButton").setAttribute("aria-label", state.running ? "暂停仿真" : "继续仿真");
      showToast(state.running ? "仿真已继续" : "仿真已暂停", state.running ? "▶" : "Ⅱ");
    });

    $$("#speedControl button").forEach((button) => {
      button.addEventListener("click", () => {
        if (backendConnected) {
          const speed = Number(button.dataset.speed);
          sendBackendControl("set_speed", speed).then((snapshot) => {
            if (snapshot) showToast(`仿真速度已切换为 ${snapshot.speed}×`);
          });
          return;
        }
        if (backendAuthoritative) return showToast("后端连接中断，正在重连", "!");
        state.speed = Number(button.dataset.speed);
        $$("#speedControl button").forEach((item) => item.classList.toggle("active", item === button));
        showToast(`仿真速度已切换为 ${state.speed}×`);
      });
    });

    $$("[data-view]").forEach((button) => {
      button.addEventListener("click", () => {
        state.view = button.dataset.view;
        $$("[data-view]").forEach((item) => item.classList.toggle("active", item === button));
        resetView();
      });
    });

    $$("#filterList [data-filter]").forEach((button) => {
      button.addEventListener("click", () => {
        state.filter = button.dataset.filter;
        $$("#filterList [data-filter]").forEach((item) => item.classList.toggle("active", item === button));
      });
    });

    $("#levelSelect").addEventListener("change", (event) => {
      state.level = event.target.value;
      showToast(state.level === "all" ? "已显示全部货架层" : `已聚焦第 ${state.level} 层货架`);
    });

    $("#routeToggle").addEventListener("click", (event) => {
      state.routes = !state.routes;
      event.currentTarget.classList.toggle("active", state.routes);
      event.currentTarget.setAttribute("aria-pressed", String(state.routes));
    });

    $("#heatToggle").addEventListener("click", (event) => {
      state.heat = !state.heat;
      event.currentTarget.classList.toggle("active", state.heat);
      event.currentTarget.setAttribute("aria-pressed", String(state.heat));
    });

    $("#flowRange").addEventListener("input", (event) => {
      state.flowRate = Number(event.target.value);
      const label = state.flowRate < 40 ? "平缓" : state.flowRate < 70 ? "中等" : "峰值";
      $("#flowValue").textContent = `${label} · ${state.flowRate} 单/时`;
    });
    $("#flowRange").addEventListener("change", () => {
      if (backendConnected) sendBackendControl("set_flow_rate", state.flowRate);
    });

    $("#robotSearch").addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      const match = event.currentTarget.value.match(/\d+/);
      const id = match ? clamp(Number(match[0]), 1, robots.length) : NaN;
      if (!Number.isFinite(id)) {
        showToast(`请输入 1—${robots.length} 的机器人编号`, "!");
        return;
      }
      state.selectedId = id;
      state.filter = "all";
      $$("#filterList [data-filter]").forEach((item) => item.classList.toggle("active", item.dataset.filter === "all"));
      updateSelectedRobot();
      showToast(`已定位 ${robotCode(id)}`);
    });

    $("#priorityTaskButton").addEventListener("click", () => {
      if (backendConnected) {
        backendPost("/api/tasks/priority").then((result) => {
          if (result.status === "assigned") {
            state.selectedId = result.robot_id;
            showToast(`优先任务已分配给 ${robotCode(result.robot_id)}`);
          } else {
            showToast("优先任务已进入等待队列");
          }
        }).catch((error) => showToast(`优先任务失败：${error.message}`, "!"));
        return;
      }
      if (backendAuthoritative) return showToast("后端连接中断，正在重连", "!");
      let robot = robots.find((item) => item.stage === "idle");
      if (!robot) robot = robots.find((item) => item.stage === "returning") || robots[0];
      prepareTask(robot);
      robot.priority = "high";
      setMovingStage(robot, "toPick");
      state.selectedId = robot.id;
      pushEvent(robot, "收到优先任务，正在前往入库台");
      updateTaskQueue(true);
      showToast(`优先任务已分配给 ${robotCode(robot.id)}`);
    });

    $("#resetButton").addEventListener("click", () => {
      if (backendConnected) {
        sendBackendControl("reset").then((snapshot) => {
          if (snapshot) {
            resetView();
            showToast("仿真场景已重置");
          }
        });
        return;
      }
      if (backendAuthoritative) return showToast("后端连接中断，正在重连", "!");
      initializeSimulation();
      resetView();
      state.running = true;
      $("#playIcon").textContent = "Ⅱ";
      showToast("仿真场景已重置");
    });

    $("#exportButton").addEventListener("click", () => {
      const rows = [
        ["robot_id", "robot_type", "max_level", "stage", "x", "y", "battery", "task", "cargo", "rack", "bay", "depth_row", "level", "slot_occupied"],
        ...robots.map((robot) => [robotCode(robot.id), robot.type.key, robot.type.maxLevel, robot.stage, robot.x.toFixed(2), robot.y.toFixed(2), robot.battery.toFixed(1), taskCode(robot), cargoCode(robot), `${robot.rack.zone}-${pad(robot.rack.id)}`, robot.bay, shelfRowName(robot.depthRow), robot.level, robot.slot?.occupied ? "yes" : "no"]),
      ];
      const csv = rows.map((row) => row.join(",")).join("\n");
      const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `robot-status-${formatClock(state.elapsed).replaceAll(":", "-")}.csv`;
      link.click();
      URL.revokeObjectURL(url);
      showToast("机器人状态已导出");
    });

    $("#zoomIn").addEventListener("click", () => changeZoom(1.15));
    $("#zoomOut").addEventListener("click", () => changeZoom(0.87));
    $("#zoomReset").addEventListener("click", resetView);

    canvas.addEventListener("wheel", (event) => {
      event.preventDefault();
      const rect = canvas.getBoundingClientRect();
      changeZoom(event.deltaY < 0 ? 1.08 : 0.92, event.clientX - rect.left, event.clientY - rect.top);
    }, { passive: false });

    canvas.addEventListener("pointerdown", (event) => {
      if (event.button > 2) return;
      isDragging = true;
      dragMoved = false;
      const mode = state.view === "iso" && event.button === 0 && !event.shiftKey ? "orbit" : "pan";
      pointerStart = {
        x: event.clientX,
        y: event.clientY,
        panX: state.panX,
        panY: state.panY,
        yaw: state.cameraYaw,
        pitch: state.cameraPitch,
        mode,
      };
      canvas.setPointerCapture(event.pointerId);
      stage.classList.add(mode === "orbit" ? "orbiting" : "dragging");
    });

    canvas.addEventListener("pointermove", (event) => {
      if (!isDragging) return;
      const dx = event.clientX - pointerStart.x;
      const dy = event.clientY - pointerStart.y;
      if (Math.hypot(dx, dy) > 4) dragMoved = true;
      if (pointerStart.mode === "orbit") {
        state.cameraYaw = pointerStart.yaw + dx * 0.009;
        state.cameraPitch = clamp(pointerStart.pitch - dy * 0.006, 0.16, 1.23);
      } else {
        state.panX = pointerStart.panX + dx;
        state.panY = pointerStart.panY + dy;
      }
    });

    canvas.addEventListener("pointerup", (event) => {
      if (!dragMoved && event.button === 0 && pointerStart.mode === "orbit") {
        selectNearestRobot(event.clientX, event.clientY);
      }
      isDragging = false;
      stage.classList.remove("dragging");
      stage.classList.remove("orbiting");
    });

    canvas.addEventListener("pointercancel", () => {
      isDragging = false;
      stage.classList.remove("dragging");
      stage.classList.remove("orbiting");
    });

    canvas.addEventListener("contextmenu", (event) => event.preventDefault());

    window.addEventListener("resize", resizeCanvas);
  }

  function boot() {
    resizeCanvas();
    initializeSimulation();
    bindInteractions();
    renderLatestEvent();
    connectBackend();
    requestAnimationFrame(animate);
  }

  boot();
})();
