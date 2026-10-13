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

  const state = {
    running: true,
    speed: 1,
    view: "iso",
    zoom: 1,
    panX: 0,
    panY: 0,
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
  let isDragging = false;
  let dragMoved = false;
  let pointerStart = { x: 0, y: 0, panX: 0, panY: 0 };

  const taskEvents = [
    { time: "08:14:32", robot: "HR-063", text: "已完成 A-08 / L2 上架" },
    { time: "08:14:18", robot: "HR-024", text: "抵达入库台，开始识别货物" },
    { time: "08:13:56", robot: "HR-081", text: "路径已重规划，绕行通道 06" },
    { time: "08:13:41", robot: "HR-012", text: "已完成 C-15 / L4 上架" },
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
  const pad = (value, size = 2) => String(value).padStart(size, "0");
  const robotCode = (id) => `HR-${pad(id, 3)}`;
  const taskCode = (robot) => `TK-${2000 + ((robot.id * 37 + robot.taskRound * 11) % 780)}`;
  const cargoCode = (robot) => `BX-${4000 + ((robot.id * 83 + robot.taskRound * 17) % 890)}`;

  function categoryFor(robot) {
    if (robot.stage === "warning") return "warning";
    if (robot.stage === "picking" || robot.stage === "placing") return "handling";
    if (robot.stage === "idle" || robot.stage === "charging") return "idle";
    return "moving";
  }

  function stageLabel(robot) {
    const labels = {
      toPick: "前往入库台",
      picking: "正在取货",
      toRack: "前往货架",
      placing: `正在执行 L${robot.level} 上架`,
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
        for (let level = 1; level <= 4; level += 1) {
          for (let bay = 0; bay < 3; bay += 1) {
            slots.push({ level, bay, occupied: random() > 0.24, colorIndex: Math.floor(random() * 4) });
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
    return { x: rack.x + 1.4, y: rack.y + 1.85 };
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
    robot.taskRound += 1;
    robot.rack = racks[(robot.id * 5 + robot.taskRound * 3) % racks.length];
    robot.level = 1 + ((robot.id + robot.taskRound) % 4);
    robot.priority = robot.priority === "high" ? "high" : pick(["normal", "normal", "normal", "medium"]);
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
    const robot = {
      id,
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
      rack: racks[0],
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
      robot.actionDuration = 1.9 + random() * 1.4;
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
    state.selectedId = 37;
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
          robot.actionDuration = 2.4 + robot.level * 0.22;
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
          state.completed += 1;
          pushEvent(robot, `已完成 ${robot.rack.zone}-${pad(robot.rack.id)} / L${robot.level} 上架`);
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

    const fitWidth = (renderWidth - 50) / ((WORLD.width + WORLD.height) * 0.5);
    const fitHeight = (renderHeight - 72) / ((WORLD.width + WORLD.height) * 0.25 + 4.6);
    const tileWidth = Math.min(fitWidth, fitHeight * 2) * state.zoom;
    const tileHeight = tileWidth * 0.5;
    const minX = (-WORLD.height * tileWidth) / 2;
    const maxX = (WORLD.width * tileWidth) / 2;
    const worldHeight = ((WORLD.width + WORLD.height) * tileHeight) / 2;
    return {
      tileWidth,
      tileHeight,
      originX: renderWidth / 2 - (minX + maxX) / 2 + state.panX,
      originY: Math.max(24, (renderHeight - worldHeight) / 2 + 28) + state.panY,
    };
  }

  function project(x, y, z = 0) {
    const metrics = projectionMetrics();
    if (state.view === "top") {
      return { x: metrics.originX + x * metrics.scale, y: metrics.originY + y * metrics.scale };
    }
    return {
      x: metrics.originX + ((x - y) * metrics.tileWidth) / 2,
      y: metrics.originY + ((x + y) * metrics.tileHeight) / 2 - z * metrics.tileHeight * 1.58,
    };
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

    pathPolygon([b2, b3, t3, t2], palette.right, stroke, 0.55);
    pathPolygon([b3, b4, t4, t3], palette.left, stroke, 0.55);
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
      for (let bay = 1; bay < 3; bay += 1) {
        const bx = p1.x + ((p2.x - p1.x) * bay) / 3;
        ctx.beginPath();
        ctx.moveTo(bx, p1.y);
        ctx.lineTo(bx, p2.y);
        ctx.stroke();
      }
      ctx.restore();
      return;
    }

    const rackHeight = 3.1;
    drawPrism(rack.x, rack.y, rack.width, rack.depth, rackHeight, {
      top: "rgba(65,82,76,0.30)",
      left: "rgba(37,49,45,0.28)",
      right: "rgba(46,60,55,0.30)",
    }, "rgba(102,122,114,0.34)");

    const boxColors = [
      { top: "#c0905f", left: "#8c6039", right: "#a57143" },
      { top: "#7399a3", left: "#45656e", right: "#547b84" },
      { top: "#91836b", left: "#635945", right: "#776a52" },
      { top: "#7b9361", left: "#52633f", right: "#63784c" },
    ];

    rack.slots.forEach((slot) => {
      if (!slot.occupied) return;
      if (state.level !== "all" && Number(state.level) !== slot.level) return;
      const levelBase = (slot.level - 1) * 0.7 + 0.09;
      drawPrism(
        rack.x + 0.12 + slot.bay * 0.88,
        rack.y + 0.12,
        0.66,
        0.77,
        0.47,
        boxColors[slot.colorIndex],
        "rgba(255,255,255,0.1)",
        levelBase,
      );
    });

    ctx.save();
    ctx.strokeStyle = state.level === "all" ? "#63766e" : COLORS.lime;
    ctx.lineWidth = 1;
    [0, rack.width].forEach((dx) => {
      [0, rack.depth].forEach((dy) => {
        const bottom = project(rack.x + dx, rack.y + dy, 0);
        const top = project(rack.x + dx, rack.y + dy, rackHeight);
        ctx.beginPath();
        ctx.moveTo(bottom.x, bottom.y);
        ctx.lineTo(top.x, top.y);
        ctx.stroke();
      });
    });
    for (let level = 0; level <= 4; level += 1) {
      const z = 0.04 + level * 0.72;
      const points = [
        project(rack.x, rack.y, z),
        project(rack.x + rack.width, rack.y, z),
        project(rack.x + rack.width, rack.y + rack.depth, z),
        project(rack.x, rack.y + rack.depth, z),
      ];
      ctx.globalAlpha = state.level === "all" || Number(state.level) === level ? 0.85 : 0.22;
      pathPolygon(points, null, "#60736b", 0.75);
    }
    const labelPoint = project(rack.x + rack.width / 2, rack.y + rack.depth, rackHeight + 0.22);
    ctx.globalAlpha = 0.72;
    ctx.fillStyle = "#aebbb6";
    ctx.font = `${Math.max(6, 7 * state.zoom)}px ui-monospace, monospace`;
    ctx.textAlign = "center";
    ctx.fillText(`${rack.zone}-${pad(rack.id)}`, labelPoint.x, labelPoint.y);
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
    const lift = robot.stage === "placing" ? Math.sin(actionProgress * Math.PI) : robot.stage === "picking" ? Math.sin(actionProgress * Math.PI) * 0.65 : 0;

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
      const hx = Math.cos(robot.heading) * 6 * scale;
      const hy = Math.sin(robot.heading) * 6 * scale;
      ctx.beginPath();
      ctx.moveTo(point.x, point.y);
      ctx.lineTo(point.x + hx, point.y + hy);
      ctx.stroke();
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

      const armY = point.y - 25 * s;
      const armRaise = lift * 8 * s;
      ctx.strokeStyle = "#96a39e";
      ctx.lineWidth = 2.1 * s;
      ctx.beginPath();
      if (robot.carrying || robot.stage === "picking" || robot.stage === "placing") {
        ctx.moveTo(point.x - 5 * s, armY);
        ctx.lineTo(point.x - 8 * s, armY + 7 * s - armRaise);
        ctx.lineTo(point.x - 4 * s, armY + 9 * s - armRaise);
        ctx.moveTo(point.x + 5 * s, armY);
        ctx.lineTo(point.x + 8 * s, armY + 7 * s - armRaise);
        ctx.lineTo(point.x + 4 * s, armY + 9 * s - armRaise);
      } else {
        ctx.moveTo(point.x - 5 * s, armY);
        ctx.lineTo(point.x - 7 * s + walk * 1.5 * s, armY + 12 * s);
        ctx.moveTo(point.x + 5 * s, armY);
        ctx.lineTo(point.x + 7 * s - walk * 1.5 * s, armY + 12 * s);
      }
      ctx.stroke();

      if (robot.carrying || robot.stage === "picking" || robot.stage === "placing") {
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
      const label = `${robotCode(robot.id)} · ${stageLabel(robot)}`;
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
      ...racks.map((rack) => ({ depth: rack.x + rack.y + rack.depth + 0.5, draw: () => drawRack(rack) })),
      ...robots.map((robot) => ({ depth: robot.x + robot.y + 0.7, draw: () => drawRobot(robot) })),
    ];

    drawables.sort((a, b) => a.depth - b.depth).forEach((item) => item.draw());
    drawWorldLabels();
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
    const lift = ["picking", "placing"].includes(robot.stage) ? Math.sin(actionProgress * Math.PI) * 22 : 0;
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
    portraitCtx.strokeStyle = "#93a09b";
    portraitCtx.lineWidth = 5;
    portraitCtx.beginPath();
    if (robot.carrying || ["picking", "placing"].includes(robot.stage)) {
      portraitCtx.moveTo(-14, 46);
      portraitCtx.lineTo(-23, 68 - lift);
      portraitCtx.lineTo(-10, 69 - lift);
      portraitCtx.moveTo(14, 46);
      portraitCtx.lineTo(23, 68 - lift);
      portraitCtx.lineTo(10, 69 - lift);
    } else {
      portraitCtx.moveTo(-14, 46);
      portraitCtx.lineTo(-21 + walk, 76);
      portraitCtx.moveTo(14, 46);
      portraitCtx.lineTo(21 - walk, 76);
    }
    portraitCtx.stroke();
    if (robot.carrying || ["picking", "placing"].includes(robot.stage)) {
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
    portraitCtx.fillText(robotCode(robot.id), 8, height - 7);
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
    const rackLabel = `${robot.rack.zone}区 · 货架 ${pad(robot.rack.id)}`;

    $("#robotIdTag").textContent = robotCode(robot.id);
    $("#selectedRobotName").textContent = `人形搬运单元 ${pad(robot.id, 3)}`;
    $("#selectedRobotPosition").textContent = `坐标 ${robot.x.toFixed(1)}, ${robot.y.toFixed(1)} · 通道 ${pad(Math.max(1, Math.round(robot.y / 2)), 2)}`;
    status.className = `state-pill ${category}`;
    status.innerHTML = `<i></i>${stageLabel(robot)}`;
    $("#batteryValue").textContent = `${Math.round(robot.battery)}%`;
    $("#batteryBar").style.width = `${robot.battery}%`;
    $("#batteryBar").style.background = robot.battery < 18 ? COLORS.red : COLORS.lime;
    $("#selectedTaskId").textContent = `#${taskCode(robot)}`;
    $("#selectedTaskTitle").textContent = `将货物送至 ${rackLabel}`;
    $("#selectedCargo").textContent = `${cargoCode(robot)} · 标准箱`;
    $("#selectedLevel").textContent = `L${robot.level}`;
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
    const waiting = Math.max(3, Math.round(state.flowRate * 0.31 - counts.idle * 0.35));
    const utilization = Math.round(active);

    $("#onlineCount").textContent = 100 - robots.filter((robot) => robot.alert === "离线").length;
    $("#activeTaskCount").textContent = active;
    $("#waitingTaskCount").textContent = waiting;
    $("#taskLoadBar").style.width = `${Math.min(100, active)}%`;
    $("#completedCount").textContent = state.completed.toLocaleString("zh-CN");
    $("#warningCount").textContent = counts.warning;
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
    $("#utilizationDonut").style.background = `conic-gradient(${COLORS.lime} 0 ${counts.moving}%, ${COLORS.blue} ${counts.moving}% ${active}%, #303a37 ${active}% 100%)`;
    $("#simClock").textContent = formatClock(state.elapsed);
    $("#connectionLatency").textContent = `${16 + Math.floor(Math.random() * 6)} ms`;
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
      .filter((robot) => robot.stage === "toPick" || robot.stage === "idle")
      .sort((a, b) => (a.priority === "high" ? -1 : b.priority === "high" ? 1 : a.id - b.id))
      .slice(0, 4);
    queue.innerHTML = items.map((robot, index) => {
      const priority = robot.priority === "high" ? "high" : index === 1 ? "medium" : "normal";
      const eta = 2 + ((robot.id * 3 + index) % 8);
      return `
        <article class="queue-item">
          <i class="queue-priority ${priority}"></i>
          <div><h4>#${taskCode(robot)} · ${robotCode(robot.id)}</h4><p>入库台 → ${robot.rack.zone}-${pad(robot.rack.id)} / L${robot.level}</p></div>
          <time>${eta}m</time>
        </article>`;
    }).join("");
  }

  function renderLatestEvent() {
    const event = taskEvents[0];
    $("#eventTicker").innerHTML = `<span class="event-time">${event.time}</span><p><strong>${event.robot}</strong> ${event.text}</p>`;
  }

  function updateAllDom(force = false) {
    updateMetrics();
    updateSelectedRobot();
    updateTaskQueue(force);
  }

  function animate(timestamp) {
    const rawDelta = Math.min(0.06, Math.max(0, (timestamp - lastTime) / 1000));
    lastTime = timestamp;
    const delta = state.running ? rawDelta * state.speed : 0;
    if (delta > 0) {
      state.elapsed += delta;
      robots.forEach((robot) => updateRobot(robot, delta));
      domAccumulator += rawDelta;
      eventAccumulator += delta;
    }

    if (domAccumulator > 0.25 || delta === 0) {
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
  }

  function changeZoom(amount, centerX = renderWidth / 2, centerY = renderHeight / 2) {
    const oldZoom = state.zoom;
    state.zoom = clamp(state.zoom * amount, 0.62, 1.9);
    const ratio = state.zoom / oldZoom;
    state.panX = centerX - (centerX - state.panX) * ratio;
    state.panY = centerY - (centerY - state.panY) * ratio;
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
      state.running = !state.running;
      $("#playIcon").textContent = state.running ? "Ⅱ" : "▶";
      $("#playButton").setAttribute("aria-label", state.running ? "暂停仿真" : "继续仿真");
      showToast(state.running ? "仿真已继续" : "仿真已暂停", state.running ? "▶" : "Ⅱ");
    });

    $$("#speedControl button").forEach((button) => {
      button.addEventListener("click", () => {
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

    $("#robotSearch").addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      const match = event.currentTarget.value.match(/\d+/);
      const id = match ? clamp(Number(match[0]), 1, 100) : NaN;
      if (!Number.isFinite(id)) {
        showToast("请输入 1—100 的机器人编号", "!");
        return;
      }
      state.selectedId = id;
      state.filter = "all";
      $$("#filterList [data-filter]").forEach((item) => item.classList.toggle("active", item.dataset.filter === "all"));
      updateSelectedRobot();
      showToast(`已定位 ${robotCode(id)}`);
    });

    $("#priorityTaskButton").addEventListener("click", () => {
      let robot = robots.find((item) => item.stage === "idle");
      if (!robot) robot = robots.find((item) => item.stage === "returning") || robots[0];
      robot.priority = "high";
      prepareTask(robot);
      setMovingStage(robot, "toPick");
      state.selectedId = robot.id;
      pushEvent(robot, "收到优先任务，正在前往入库台");
      updateTaskQueue(true);
      showToast(`优先任务已分配给 ${robotCode(robot.id)}`);
    });

    $("#resetButton").addEventListener("click", () => {
      initializeSimulation();
      resetView();
      state.running = true;
      $("#playIcon").textContent = "Ⅱ";
      showToast("仿真场景已重置");
    });

    $("#exportButton").addEventListener("click", () => {
      const rows = [
        ["robot_id", "stage", "x", "y", "battery", "task", "cargo", "destination", "level"],
        ...robots.map((robot) => [robotCode(robot.id), robot.stage, robot.x.toFixed(2), robot.y.toFixed(2), robot.battery.toFixed(1), taskCode(robot), cargoCode(robot), `${robot.rack.zone}-${pad(robot.rack.id)}`, robot.level]),
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
      isDragging = true;
      dragMoved = false;
      pointerStart = { x: event.clientX, y: event.clientY, panX: state.panX, panY: state.panY };
      canvas.setPointerCapture(event.pointerId);
      stage.classList.add("dragging");
    });

    canvas.addEventListener("pointermove", (event) => {
      if (!isDragging) return;
      const dx = event.clientX - pointerStart.x;
      const dy = event.clientY - pointerStart.y;
      if (Math.hypot(dx, dy) > 4) dragMoved = true;
      state.panX = pointerStart.panX + dx;
      state.panY = pointerStart.panY + dy;
    });

    canvas.addEventListener("pointerup", (event) => {
      if (!dragMoved) selectNearestRobot(event.clientX, event.clientY);
      isDragging = false;
      stage.classList.remove("dragging");
    });

    canvas.addEventListener("pointercancel", () => {
      isDragging = false;
      stage.classList.remove("dragging");
    });

    window.addEventListener("resize", resizeCanvas);
  }

  function boot() {
    resizeCanvas();
    initializeSimulation();
    bindInteractions();
    renderLatestEvent();
    requestAnimationFrame(animate);
  }

  boot();
})();
