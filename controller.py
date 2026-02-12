#!/usr/bin/env python

"""This is the Controller Starter Code for ECE50863 Lab Project 1
Author: Xin Du
Email: du201@purdue.edu
Last Modified Date: December 9th, 2021
"""

import sys
import socket
import heapq
import threading
import time
from datetime import date, datetime

# Please do not modify the name of the log file, otherwise you will lose points because the grader won't be able to find your log file
LOG_FILE = "Controller.log"
K = 2
TIMEOUT = 3 * K

# Those are logging functions to help you follow the correct logging standard

# "Register Request" Format is below:
#
# Timestamp
# Register Request <Switch-ID>

def register_request_received(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Request {switch_id}\n")
    write_to_log(log)

# "Register Responses" Format is below (for every switch):
#
# Timestamp
# Register Response <Switch-ID>

def register_response_sent(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Response {switch_id}\n")
    write_to_log(log) 

# For the parameter "routing_table", it should be a list of lists in the form of [[...], [...], ...]. 
# Within each list in the outermost list, the first element is <Switch ID>. The second is <Dest ID>, and the third is <Next Hop>, and the fourth is <Shortest distance>
# "Routing Update" Format is below:
#
# Timestamp
# Routing Update 
# <Switch ID>,<Dest ID>:<Next Hop>,<Shortest distance>
# ...
# ...
# Routing Complete
#
# You should also include all of the Self routes in your routing_table argument -- e.g.,  Switch (ID = 4) should include the following entry: 		
# 4,4:4,0
# 0 indicates ‘zero‘ distance
#
# For switches that can’t be reached, the next hop and shortest distance should be ‘-1’ and ‘9999’ respectively. (9999 means infinite distance so that that switch can’t be reached)
#  E.g, If switch=4 cannot reach switch=5, the following should be printed
#  4,5:-1,9999
#
# For any switch that has been killed, do not include the routes that are going out from that switch. 
# One example can be found in the sample log in starter code. 
# After switch 1 is killed, the routing update from the controller does not have routes from switch 1 to other switches.

def routing_table_update(routing_table):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append("Routing Update\n")
    for row in routing_table:
        log.append(f"{row[0]},{row[1]}:{row[2]},{row[3]}\n")
    log.append("Routing Complete\n")
    write_to_log(log)

# "Topology Update: Link Dead" Format is below: (Note: We do not require you to print out Link Alive log in this project)
#
#  Timestamp
#  Link Dead <Switch ID 1>,<Switch ID 2>

def topology_update_link_dead(switch_id_1, switch_id_2):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Link Dead {switch_id_1},{switch_id_2}\n")
    write_to_log(log) 

# "Topology Update: Switch Dead" Format is below:
#
#  Timestamp
#  Switch Dead <Switch ID>

def topology_update_switch_dead(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Switch Dead {switch_id}\n")
    write_to_log(log) 

# "Topology Update: Switch Alive" Format is below:
#
#  Timestamp
#  Switch Alive <Switch ID>

def topology_update_switch_alive(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Switch Alive {switch_id}\n")
    write_to_log(log) 

def write_to_log(log):
    with open(LOG_FILE, 'a+') as log_file:
        log_file.write("\n\n")
        # Write to log
        log_file.writelines(log)

def main():
    #Check for number of arguments and exit if host/port not provided
    num_args = len(sys.argv)
    if num_args < 3:
        print ("Usage: python controller.py <port> <config file>\n")
        sys.exit(1)
    
    # Write your code below or elsewhere in this file

    # Parameters
    port = int(sys.argv[1])
    config = sys.argv[2]

    # Variables
    switch_cnt = 0
    switch_addresses = {}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', port))
    sock.settimeout(1.0)
    topology = {}
    alive_switches = set() 
    last_heard = {} 
    neighbor_reports = {}
    dead_links = set()          
    lock = threading.Lock()

    # ========== Functions ==========

    def send_register_response(sid):
        """Send Register Response to a switch with all its configured neighbors."""
        neighbors = topology.get(sid, {})
        resp_lines = ["REGISTER_RESPONSE", str(len(neighbors))]
        for nid in sorted(neighbors.keys()):
            if nid in alive_switches and nid in switch_addresses:
                na = switch_addresses[nid]
                resp_lines.append(f"{nid} True {na[0]} {na[1]}")
            else:
                resp_lines.append(f"{nid} False")
        sock.sendto("\n".join(resp_lines).encode(), switch_addresses[sid])

    def compute_routes():
        """Compute shortest paths using Dijkstra on the effective topology."""
        eff_topo = {}
        for sid in alive_switches:
            eff_topo[sid] = {}
            for nid, dist in topology.get(sid, {}).items():
                if nid in alive_switches:
                    lk = (min(sid, nid), max(sid, nid))
                    if lk not in dead_links:
                        eff_topo[sid][nid] = dist

        all_routes = []
        switch_tables = {}

        for src in sorted(alive_switches):
            switch_tables[src] = []

            # Dijkstra
            dists = {s: float('inf') for s in alive_switches}
            dists[src] = 0
            parents = {s: None for s in alive_switches}
            pq = [(0, src)]
            visited = set()

            while pq:
                d, u = heapq.heappop(pq)
                if u in visited:
                    continue
                visited.add(u)
                for v, w in eff_topo.get(u, {}).items():
                    if v not in visited and dists[u] + w < dists[v]:
                        dists[v] = dists[u] + w
                        parents[v] = u
                        heapq.heappush(pq, (dists[v], v))

            # Build routing table entries for this source
            for dest in range(switch_cnt):
                if dest == src:
                    all_routes.append([src, dest, src, 0])
                    switch_tables[src].append(f"{dest} {src} 0")
                elif dest not in alive_switches or dists.get(dest, float('inf')) == float('inf'):
                    all_routes.append([src, dest, -1, 9999])
                    switch_tables[src].append(f"{dest} -1 9999")
                else:
                    d_val = dists[dest]
                    nh = dest
                    while parents[nh] is not None and parents[nh] != src:
                        nh = parents[nh]
                    all_routes.append([src, dest, nh, d_val])
                    switch_tables[src].append(f"{dest} {nh} {d_val}")

        return all_routes, switch_tables

    def compute_and_send_routes():
        """Compute routes, log them, and send to all alive switches."""
        all_routes, switch_tables = compute_routes()
        routing_table_update(all_routes)
        for sid in alive_switches:
            if sid in switch_addresses and sid in switch_tables:
                msg_lines = ["ROUTE_UPDATE", str(sid)]
                msg_lines.extend(switch_tables[sid])
                sock.sendto("\n".join(msg_lines).encode(), switch_addresses[sid])
    
    def receiver():
        """Thread to receive messages from switches."""
        while True:
            try:
                data, addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except ConnectionResetError:
                continue
            except OSError:
                break

            msg = data.decode().strip()
            msg_lines = msg.split('\n')

            with lock:
                first_parts = msg_lines[0].split()
                if len(first_parts) >= 2 and first_parts[1] == "Register_Request":
                    sid = int(first_parts[0])
                    register_request_received(sid)
                    switch_addresses[sid] = addr
                    last_heard[sid] = time.time()

                    if sid not in alive_switches:
                        alive_switches.add(sid)
                        topology_update_switch_alive(sid)
                        # Reset neighbor reports for this switch (optimistic: all alive)
                        neighbor_reports[sid] = {}
                        for nid in topology.get(sid, {}):
                            neighbor_reports[sid][nid] = True
                        # Also reset other switches' reports about this switch
                        for other_sid in alive_switches:
                            if sid in neighbor_reports.get(other_sid, {}):
                                neighbor_reports[other_sid][sid] = True
                        # Clear dead links involving this switch
                        dead_links.difference_update(
                            {lk for lk in dead_links if sid in lk}
                        )
                    # Send Register Response
                    send_register_response(sid)
                    register_response_sent(sid)
                    # Recompute and send routes
                    compute_and_send_routes()
                elif msg_lines[0] == "TOPOLOGY_UPDATE":
                    sid = int(msg_lines[1])
                    last_heard[sid] = time.time()
                    # Update neighbor reports
                    for line in msg_lines[2:]:
                        lparts = line.split()
                        if len(lparts) >= 2:
                            nid = int(lparts[0])
                            is_alive = lparts[1] == "True"
                            neighbor_reports.setdefault(sid, {})[nid] = is_alive

    def periodic():
        """Thread to periodically check switch and link status."""
        while True:
            time.sleep(K)
            with lock:
                now = time.time()
                changed = False
                # Detect dead switches
                newly_dead = []
                for sid in list(alive_switches):
                    if now - last_heard.get(sid, 0) > TIMEOUT:
                        newly_dead.append(sid)
                for sid in newly_dead:
                    alive_switches.discard(sid)
                    topology_update_switch_dead(sid)
                    dead_links.difference_update({lk for lk in dead_links if sid in lk})
                    changed = True
                # Detect link changes among alive switches
                cur_dead_links = set()
                for sid in alive_switches:
                    for nid, is_alive in neighbor_reports.get(sid, {}).items():
                        if nid in alive_switches and not is_alive:
                            cur_dead_links.add((min(sid, nid), max(sid, nid)))
                # New dead links
                for lk in cur_dead_links - dead_links:
                    topology_update_link_dead(lk[0], lk[1])
                    changed = True
                # Revived links
                if dead_links - cur_dead_links:
                    changed = True
                dead_links.clear()
                dead_links.update(cur_dead_links)
                # Recompute routes if topology changed
                if changed:
                    compute_and_send_routes()
    
    # ============================================

    # Read Configuration
    try:
        with open(config, 'r') as f:
            lines = [l.strip() for l in f.readlines() if l.strip()]
            switch_cnt = int(lines[0])
            topology = {i: {} for i in range(switch_cnt)}
            
            for line in lines[1:]:
                parts = line.split()
                if len(parts) < 3: continue
                u, v, dist = int(parts[0]), int(parts[1]), int(parts[2])
                topology[u][v] = dist
                topology[v][u] = dist
    except Exception as e:
        print(f"Error read file: {e}")
        sys.exit(1)
    
    # Wait for Registration
    while len(switch_addresses) < switch_cnt:
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
            continue
        msg = data.decode().strip()
        parts = msg.split()
        if len(parts) >= 2 and parts[1] == "Register_Request":
            sid = int(parts[0])
            register_request_received(sid)
            switch_addresses[sid] = addr
            alive_switches.add(sid)
            last_heard[sid] = time.time()

    # Send Register Responses to all switches
    for sid in range(switch_cnt):
        send_register_response(sid)
        register_response_sent(sid)

    # Initialize neighbor reports (all neighbors alive)
    for sid in range(switch_cnt):
        neighbor_reports[sid] = {}
        for nid in topology.get(sid, {}):
            neighbor_reports[sid][nid] = True

    # Update last_heard after responses sent and send initial routes
    now = time.time()
    for sid in range(switch_cnt):
        last_heard[sid] = now
    compute_and_send_routes()

    # Start Threads
    recv_thread = threading.Thread(target=receiver, daemon=True)
    per_thread = threading.Thread(target=periodic, daemon=True)
    recv_thread.start()
    per_thread.start()

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()