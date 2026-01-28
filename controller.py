#!/usr/bin/env python

"""This is the Controller Starter Code for ECE50863 Lab Project 1
Author: Xin Du
Email: du201@purdue.edu
Last Modified Date: December 9th, 2021
"""

import sys
import socket
import heapq
from datetime import date, datetime

# Please do not modify the name of the log file, otherwise you will lose points because the grader won't be able to find your log file
LOG_FILE = "Controller.log"

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
    topology = {}
    
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
        data, addr = sock.recvfrom(4096)
        parts = data.decode().strip().split()
        if len(parts) >= 2 and parts[1] == "Register_Request":
            switch_id = int(parts[0])
            if switch_id not in switch_addresses:
                register_request_received(switch_id)
            switch_addresses[switch_id] = addr

    # Send Register Response
    for switch_id in range(switch_cnt):
         neighbors = topology[switch_id]
         response_lines = []
         
         valid_neighbors = []
         for neighbor_id in neighbors:
             if neighbor_id in switch_addresses:
                 nb_addr = switch_addresses[neighbor_id]
                 valid_neighbors.append(f"{neighbor_id} {nb_addr[0]} {nb_addr[1]}")
         
         response_lines.append("REGISTER_RESPONSE")
         response_lines.append(str(len(valid_neighbors)))
         response_lines.extend(valid_neighbors)
         
         response_msg = "\n".join(response_lines)
         sock.sendto(response_msg.encode(), switch_addresses[switch_id])
         register_response_sent(switch_id)

    # Routing Table Calculation
    all_routes_log = []
    switch_routing_tables = {i: [] for i in range(switch_cnt)}

    for src in range(switch_cnt):
        # Dijkstra's Algorithm
        dists = {i: float('inf') for i in range(switch_cnt)}
        dists[src] = 0
        parents = {i: None for i in range(switch_cnt)}
        pq = [(0, src)]
        visited = set()
        
        while pq:
            d, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)
            if d > dists[u]:
                continue
            for v, weight in topology[u].items():
                if dists[u] + weight < dists[v]:
                    dists[v] = dists[u] + weight
                    parents[v] = u
                    heapq.heappush(pq, (dists[v], v))
        
        # Construct Routing Table
        for dest in range(switch_cnt):
            next_hop = -1
            dist_val = 9999
            
            if dest == src:
                next_hop = src
                dist_val = 0
            elif dists[dest] == float('inf'):
                next_hop = -1
                dist_val = 9999
            else:
                dist_val = dists[dest]
                next_hop = dest
                while parents[next_hop] != src and parents[next_hop] is not None:
                    next_hop = parents[next_hop]
            
            all_routes_log.append([src, dest, next_hop, dist_val])
            switch_routing_tables[src].append(f"{dest} {next_hop} {dist_val}")

    routing_table_update(all_routes_log)

    # Send Route Updates to Switches
    for switch_id in range(switch_cnt):
        lines = []
        lines.append("ROUTE_UPDATE")
        lines.append(str(switch_id)) 
        lines.extend(switch_routing_tables[switch_id])
        sock.sendto("\n".join(lines).encode(), switch_addresses[switch_id])

    while True:
        try:
             data, addr = sock.recvfrom(4096)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()