import asyncio
import json
import websockets
from flask import Flask, request, jsonify
import threading
import math
import random
import time

app = Flask(__name__)

# --- CORS ---
@app.after_request
def add_cors_headers(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Methods'] = 'GET,POST,OPTIONS'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return resp

# ---------------------------
# Globals
# ---------------------------
connected = set()
async_loop = None

# collision tracking
collision_count = 0
collision_timestamps = []  # list of monotonic times when collisions occurred
COLLISION_WINDOW = 4.0     # seconds to consider collisions "recent"
STUCK_THRESHOLD = 5        # collisions within COLLISION_WINDOW => stuck

FLOOR_HALF = 50
goal_position = None
navigating_task = None            # concurrent.futures.Future returned by run_coroutine_threadsafe
goal_reached_flag = False

# Robot pose estimate (server-side)
robot_position = {"x": 0.0, "z": 0.0}  # y is always 0 for your flat floor
robot_heading = 0.0  # degrees, 0 means facing +Z (matches index.html convention)

def corner_to_coords(corner: str, margin=5):
    c = corner.upper()
    x = FLOOR_HALF - margin if "E" in c else -(FLOOR_HALF - margin)
    z = FLOOR_HALF - margin if ("S" in c or "B" in c) else -(FLOOR_HALF - margin)
    if c in ("NE", "EN", "TR"): x, z = (FLOOR_HALF - margin, -(FLOOR_HALF - margin))
    if c in ("NW", "WN", "TL"): x, z = (-(FLOOR_HALF - margin), -(FLOOR_HALF - margin))
    if c in ("SE", "ES", "BR"): x, z = (FLOOR_HALF - margin, (FLOOR_HALF - margin))
    if c in ("SW", "WS", "BL"): x, z = (-(FLOOR_HALF - margin), (FLOOR_HALF - margin))
    return {"x": x, "y": 0, "z": z}

# ---------------------------
# WebSocket Handler
# ---------------------------
async def ws_handler(websocket, path=None):
    """
    Handles incoming messages from the simulator:
      - collision messages: record collision + timestamp
      - goal_reached: mark goal reached
      - goal_set: store goal_position (simulator pressed random button)
    When a goal_set arrives we auto-start navigation (cancelling previous task).
    """
    global collision_count, goal_reached_flag, goal_position, collision_timestamps, navigating_task, async_loop
    print("Client connected via WebSocket")
    connected.add(websocket)
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                if isinstance(data, dict):
                    tnow = time.monotonic()
                    if data.get("type") == "collision" and data.get("collision"):
                        collision_count += 1
                        collision_timestamps.append(tnow)
                        cutoff = tnow - COLLISION_WINDOW
                        collision_timestamps[:] = [ts for ts in collision_timestamps if ts >= cutoff]
                        print("Collision detected:", data, "recent_collisions:", len(collision_timestamps))

                    elif data.get("type") == "goal_reached":
                        goal_reached_flag = True
                        print("Goal reached at:", data.get("position"))

                    elif data.get("type") == "goal_set":
                        # Simulator placed a flag (random button) -> adopt as goal and auto-start navigation
                        goal_position = data.get("position")
                        goal_reached_flag = False
                        # reset collision history for fresh navigation
                        collision_timestamps.clear()
                        collision_count = 0
                        print("Goal set from simulator:", goal_position)

                        # Cancel existing navigation if running
                        if navigating_task and not navigating_task.done():
                            try:
                                navigating_task.cancel()
                                print("Old navigation cancelled (ws goal_set)")
                            except Exception:
                                pass

                        # Start new navigation task on the async_loop
                        if async_loop:
                            navigating_task = asyncio.run_coroutine_threadsafe(navigate_loop(), async_loop)
            except Exception as e:
                print("Error parsing ws message:", e)
            # debug print
            print("Received from simulator:", message)
    except websockets.exceptions.ConnectionClosed:
        print("Client disconnected")
    finally:
        connected.remove(websocket)

def broadcast(msg: dict):
    """Send a JSON message to all connected simulator clients (non-blocking)."""
    if not connected:
        return False
    for ws in list(connected):
        asyncio.run_coroutine_threadsafe(ws.send(json.dumps(msg)), async_loop)
    return True

# ---------------------------
# REST Endpoints (control)
# ---------------------------
@app.route('/move', methods=['POST'])
def move():
    data = request.get_json()
    if not data or 'x' not in data or 'z' not in data:
        return jsonify({'error': 'Missing parameters'}), 400
    msg = {"command": "move", "target": {"x": data['x'], "y": 0, "z": data['z']}}
    if not broadcast(msg):
        return jsonify({'error': 'No connected simulators.'}), 400
    return jsonify({'status': 'move command sent', 'command': msg})

@app.route('/move_rel', methods=['POST'])
def move_rel():
    data = request.get_json()
    if not data or 'turn' not in data or 'distance' not in data:
        return jsonify({'error': 'Missing parameters'}), 400
    msg = {"command": "move_relative", "turn": data['turn'], "distance": data['distance']}
    if not broadcast(msg):
        return jsonify({'error': 'No connected simulators.'}), 400
    return jsonify({'status': 'move relative command sent', 'command': msg})

@app.route('/stop', methods=['POST'])
def stop():
    msg = {"command": "stop"}
    if not broadcast(msg):
        return jsonify({'error': 'No connected simulators.'}), 400
    return jsonify({'status': 'stop command sent'})

@app.route('/capture', methods=['POST'])
def capture():
    msg = {"command": "capture_image"}
    if not broadcast(msg):
        return jsonify({'error': 'No connected simulators.'}), 400
    return jsonify({'status': 'capture command sent'})

# ---------------------------
# Goal Management (REST)
# ---------------------------
@app.route('/goal', methods=['POST'])
def set_goal():
    """
    Set a goal via REST. Cancels any running navigation and starts a new navigation task.
    """
    global goal_position, goal_reached_flag, collision_count, collision_timestamps, navigating_task, async_loop
    data = request.get_json() or {}
    if 'corner' in data:
        pos = corner_to_coords(str(data['corner']))
    elif 'x' in data and 'z' in data:
        pos = {"x": float(data['x']), "y": float(data.get('y', 0)), "z": float(data['z'])}
    else:
        return jsonify({'error': 'Invalid goal format'}), 400

    goal_position = pos
    goal_reached_flag = False
    collision_count = 0
    collision_timestamps.clear()

    msg = {"command": "set_goal", "position": pos}
    if not broadcast(msg):
        return jsonify({'error': 'No connected simulators.'}), 400

    # Cancel previous navigation if running
    if navigating_task and not navigating_task.done():
        try:
            navigating_task.cancel()
            print("Old navigation cancelled (REST /goal)")
        except Exception:
            pass

    # Start navigation (schedule on async_loop)
    if async_loop:
        navigating_task = asyncio.run_coroutine_threadsafe(navigate_loop(), async_loop)

    return jsonify({'status': 'goal set & navigation started', 'goal': pos})

@app.route('/current_goal', methods=['GET'])
def get_current_goal():
    if not goal_position:
        return jsonify({'goal': None, 'status': 'no goal set'})
    return jsonify({'goal': goal_position, 'status': 'active'})

# ---------------------------
# Navigation routine (pose-tracking)
# ---------------------------
async def navigate_loop():
    """
    Navigation loop with:
      - Pose tracking (robot_position, robot_heading)
      - Aggressive avoidance: backup + ±60° turns
      - Adaptive stuck recovery: bigger backup + alternating 90° turns
      - Only updates pose if move succeeds (no collision)
    """
    global goal_position, goal_reached_flag, collision_count
    global robot_position, robot_heading, collision_timestamps

    if not goal_position:
        print("navigate_loop: no goal set, aborting")
        return

    print("🚀 Starting navigation toward:", goal_position)

    step_distance = 1.5        # forward step
    forward_wait = 1.0
    turn_wait = 0.6
    tiny_move_for_turn = 0.01  # tiny forward with turns
    stuck_recovery_count = 0   # how many times we've triggered stuck recovery
    turn_direction = 1         # alternate avoidance turns (1=right, -1=left)

    try:
        while goal_position and not goal_reached_flag:
            # Distance to goal
            dx = goal_position["x"] - robot_position["x"]
            dz = goal_position["z"] - robot_position["z"]
            dist = math.hypot(dx, dz)

            if dist <= 1.5:
                print(f"Near goal (dist={dist:.2f}), waiting for 'goal_reached'...")
                await asyncio.sleep(0.8)
                continue

            # Desired heading
            target_angle = math.degrees(math.atan2(dx, dz))
            turn_needed = target_angle - robot_heading
            while turn_needed <= -180: turn_needed += 360
            while turn_needed > 180: turn_needed -= 360

            # --- Rotate toward goal
            if abs(turn_needed) > 6:
                broadcast({"command": "move_relative", "turn": turn_needed, "distance": tiny_move_for_turn})
                await asyncio.sleep(turn_wait)
                robot_heading = (robot_heading + turn_needed) % 360

            # --- Move forward
            pre_coll = collision_count
            broadcast({"command": "move_relative", "turn": 0, "distance": step_distance})
            await asyncio.sleep(forward_wait)

            if collision_count == pre_coll:  # success
                rad = math.radians(robot_heading)
                robot_position["x"] += step_distance * math.sin(rad)
                robot_position["z"] += step_distance * math.cos(rad)
                print(f"Moved forward → pose: {robot_position}, heading={robot_heading:.1f}")
                stuck_recovery_count = 0  # reset after success
                continue

            # --- COLLISION ---
            print("⚠️ Collision → avoidance")
            broadcast({"command": "stop"})
            await asyncio.sleep(0.2)

            # Count recent collisions
            now = time.monotonic()
            cutoff = now - COLLISION_WINDOW
            collision_timestamps[:] = [ts for ts in collision_timestamps if ts >= cutoff]
            recent_collisions = len(collision_timestamps)

            if recent_collisions >= 3:
                # === STUCK RECOVERY ===
                stuck_recovery_count += 1
                back_dist = min(2.0 * stuck_recovery_count, 6.0)  # grow backup adaptively
                print(f"🚨 STUCK RECOVERY #{stuck_recovery_count}: backup {back_dist}, turn 90°")

                # Backup
                broadcast({"command": "move_relative", "turn": 0, "distance": -back_dist})
                await asyncio.sleep(forward_wait)

                # Alternate 90° turns
                turn_angle = 90 * turn_direction
                broadcast({"command": "move_relative", "turn": turn_angle, "distance": tiny_move_for_turn})
                await asyncio.sleep(turn_wait)
                robot_heading = (robot_heading + turn_angle) % 360
                turn_direction *= -1  # alternate next time

                # Strong push
                push_dist = 6.0
                broadcast({"command": "move_relative", "turn": 0, "distance": push_dist})
                await asyncio.sleep(forward_wait + 0.5)
                rad = math.radians(robot_heading)
                robot_position["x"] += push_dist * math.sin(rad)
                robot_position["z"] += push_dist * math.cos(rad)

                collision_timestamps.clear()
                collision_count = 0
                continue

            # === NORMAL AVOIDANCE ===
            print("➡️ Normal avoidance: backup + ±60° turn + sidestep")

            # Backup
            back_dist = 1.5
            broadcast({"command": "move_relative", "turn": 0, "distance": -back_dist})
            await asyncio.sleep(forward_wait)

            # Turn ±60° (alternate sides)
            turn_angle = 60 * turn_direction
            broadcast({"command": "move_relative", "turn": turn_angle, "distance": tiny_move_for_turn})
            await asyncio.sleep(turn_wait)
            robot_heading = (robot_heading + turn_angle) % 360
            turn_direction *= -1  # alternate next time

            # Sidestep
            side_dist = 3.0
            broadcast({"command": "move_relative", "turn": 0, "distance": side_dist})
            await asyncio.sleep(forward_wait + 0.3)
            rad = math.radians(robot_heading)
            robot_position["x"] += side_dist * math.sin(rad)
            robot_position["z"] += side_dist * math.cos(rad)

            # Recalculate heading toward goal immediately
            dx = goal_position["x"] - robot_position["x"]
            dz = goal_position["z"] - robot_position["z"]
            target_angle = math.degrees(math.atan2(dx, dz))
            turn_to_goal = target_angle - robot_heading
            while turn_to_goal <= -180: turn_to_goal += 360
            while turn_to_goal > 180: turn_to_goal -= 360
            broadcast({"command": "move_relative", "turn": turn_to_goal, "distance": tiny_move_for_turn})
            await asyncio.sleep(turn_wait)
            robot_heading = (robot_heading + turn_to_goal) % 360

            collision_timestamps.clear()
            collision_count = 0

    except asyncio.CancelledError:
        print("❌ Navigation cancelled")
        try:
            broadcast({"command": "stop"})
        except Exception:
            pass
        return

    if goal_reached_flag:
        print("✅ Goal reached!")
    else:
        print("⚠️ Navigation aborted")

# ---------------------------
# Navigation trigger endpoint (kept for backward compatibility)
# ---------------------------
@app.route('/navigate_to_goal', methods=['POST'])
def start_navigation():
    global navigating_task, async_loop
    if not goal_position:
        return jsonify({'error': 'No goal set'}), 400
    if navigating_task and not navigating_task.done():
        return jsonify({'status': 'already navigating'})
    if async_loop:
        navigating_task = asyncio.run_coroutine_threadsafe(navigate_loop(), async_loop)
        return jsonify({'status': 'navigation started', 'goal': goal_position})
    return jsonify({'error': 'server not ready'}), 500

# ---------------------------
# Collisions & Reset
# ---------------------------
@app.route('/collisions', methods=['GET'])
def get_collisions():
    return jsonify({'count': collision_count, 'recent': len(collision_timestamps)})

@app.route('/reset', methods=['POST'])
def reset():
    global collision_count, goal_position, goal_reached_flag, collision_timestamps, robot_position, robot_heading, navigating_task
    collision_count = 0
    collision_timestamps.clear()
    goal_position = None
    goal_reached_flag = False
    robot_position = {"x": 0.0, "z": 0.0}
    robot_heading = 0.0
    # cancel navigation if running
    if navigating_task and not navigating_task.done():
        try:
            navigating_task.cancel()
        except Exception:
            pass
    if not broadcast({"command": "reset"}):
        return jsonify({'status': 'reset done (no simulators connected)', 'collisions': collision_count})
    return jsonify({'status': 'reset broadcast', 'collisions': collision_count})

# ---------------------------
# Flask Thread
# ---------------------------
def start_flask():
    app.run(port=5000)

# ---------------------------
# Main Async for WebSocket
# ---------------------------
async def main():
    global async_loop
    async_loop = asyncio.get_running_loop()
    ws_server = await websockets.serve(ws_handler, "localhost", 8080)
    print("WebSocket server started on ws://localhost:8080")
    await ws_server.wait_closed()

# ---------------------------
# Entry point
# ---------------------------
if __name__ == "__main__":
    flask_thread = threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()
    asyncio.run(main())
