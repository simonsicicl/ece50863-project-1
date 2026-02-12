#!/usr/bin/env python

"""This is the Switch Starter Code for ECE50863 Lab Project 1
Author: Xin Du
Email: du201@purdue.edu
Last Modified Date: December 9th, 2021
"""

import sys
import socket
import threading
import time
from datetime import date, datetime

# Please do not modify the name of the log file, otherwise you will lose points because the grader won't be able to find your log file
LOG_FILE = "switch#.log" # The log file for switches are switch#.log, where # is the id of that switch (i.e. switch0.log, switch1.log). The code for replacing # with a real number has been given to you in the main function.
K = 2
TIMEOUT = 3 * K

# Those are logging functions to help you follow the correct logging standard

# "Register Request" Format is below:
#
# Timestamp
# Register Request Sent

def register_request_sent():
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Request Sent\n")
    write_to_log(log)

# "Register Response" Format is below:
#
# Timestamp
# Register Response Received

def register_response_received():
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Register Response received\n")
    write_to_log(log) 

# For the parameter "routing_table", it should be a list of lists in the form of [[...], [...], ...]. 
# Within each list in the outermost list, the first element is <Switch ID>. The second is <Dest ID>, and the third is <Next Hop>.
# "Routing Update" Format is below:
#
# Timestamp
# Routing Update 
# <Switch ID>,<Dest ID>:<Next Hop>
# ...
# ...
# Routing Complete
# 
# You should also include all of the Self routes in your routing_table argument -- e.g.,  Switch (ID = 4) should include the following entry: 		
# 4,4:4

def routing_table_update(routing_table):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append("Routing Update\n")
    for row in routing_table:
        log.append(f"{row[0]},{row[1]}:{row[2]}\n")
    log.append("Routing Complete\n")
    write_to_log(log)

# "Unresponsive/Dead Neighbor Detected" Format is below:
#
# Timestamp
# Neighbor Dead <Neighbor ID>

def neighbor_dead(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Neighbor Dead {switch_id}\n")
    write_to_log(log) 

# "Unresponsive/Dead Neighbor comes back online" Format is below:
#
# Timestamp
# Neighbor Alive <Neighbor ID>

def neighbor_alive(switch_id):
    log = []
    log.append(str(datetime.time(datetime.now())) + "\n")
    log.append(f"Neighbor Alive {switch_id}\n")
    write_to_log(log) 

def write_to_log(log):
    with open(LOG_FILE, 'a+') as log_file:
        log_file.write("\n\n")
        # Write to log
        log_file.writelines(log)

def main():

    global LOG_FILE

    #Check for number of arguments and exit if host/port not provided
    num_args = len(sys.argv)
    if num_args < 4:
        print ("switch.py <Id_self> <Controller hostname> <Controller Port>\n")
        sys.exit(1)

    my_id = int(sys.argv[1])
    LOG_FILE = 'switch' + str(my_id) + ".log" 

    # Write your code below or elsewhere in this file

    # Parameters
    ctrl_host = sys.argv[2]
    ctrl_port = int(sys.argv[3])
    failed_neighbor = None
    if num_args >= 6 and sys.argv[4] == '-f':
        failed_neighbor = int(sys.argv[5])

    # Variables
    controller_addr = (ctrl_host, ctrl_port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', 0))
    sock.sendto(f"{my_id} Register_Request".encode(), controller_addr)
    register_request_sent()
    nb_addrs = {} 
    nb_alive = {}
    nb_last_ka = {}  
    lock = threading.Lock()

    # ========== Functions ==========

    def send_topo_update():
        """Send topology update to controller"""
        msg_lines = ["TOPOLOGY_UPDATE", str(my_id)]
        for nid in sorted(nb_alive.keys()):
            msg_lines.append(f"{nid} {nb_alive[nid]}")
        try:
            sock.sendto("\n".join(msg_lines).encode(), controller_addr)
        except (ConnectionResetError, OSError):
            pass

    def receiver():
        """Thread function to receive messages from controller and neighbors"""
        sock.settimeout(1.0)
        while True:
            try:
                data, addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except ConnectionResetError:
                continue
            except OSError:
                break

            lines = data.decode().strip().split('\n')

            with lock:
                if lines[0] == "ROUTE_UPDATE":
                    # Parse and log routing table
                    table = []
                    for line in lines[2:]:
                        parts = line.split()
                        table.append([my_id, int(parts[0]), int(parts[1])])
                    routing_table_update(table)

                elif lines[0] == "REGISTER_RESPONSE":
                    # Handle re-registration response
                    register_response_received()
                    num_nb = int(lines[1])
                    nb_addrs.clear()
                    nb_alive.clear()
                    nb_last_ka.clear()
                    now = time.time()
                    for i in range(num_nb):
                        parts = lines[2 + i].split()
                        nid = int(parts[0])
                        is_alive_flag = parts[1] == "True"
                        if is_alive_flag and len(parts) >= 4:
                            nb_addrs[nid] = (parts[2], int(parts[3]))
                            nb_alive[nid] = True
                        else:
                            nb_addrs[nid] = None
                            nb_alive[nid] = False
                        nb_last_ka[nid] = now

                else:
                    # Check for KEEP_ALIVE message
                    parts = lines[0].split()
                    if len(parts) >= 2 and parts[1] == "KEEP_ALIVE":
                        sender_id = int(parts[0])
                        # Ignore if from failed-link neighbor
                        if failed_neighbor is not None and sender_id == failed_neighbor:
                            continue
                        if sender_id in nb_alive:
                            nb_last_ka[sender_id] = time.time()
                            nb_addrs[sender_id] = addr  # update address
                            if not nb_alive[sender_id]:
                                # Neighbor came back alive
                                nb_alive[sender_id] = True
                                neighbor_alive(sender_id)
                                send_topo_update()

    def periodic():
        """Thread function to perform periodic tasks such as sending keep-alives and topology updates"""
        while True:
            time.sleep(K)
            with lock:
                now = time.time()
                topo_changed = False

                # Check for dead neighbors (timeout)
                for nid in list(nb_alive.keys()):
                    if nb_alive[nid]:
                        if now - nb_last_ka.get(nid, 0) > TIMEOUT:
                            nb_alive[nid] = False
                            neighbor_dead(nid)
                            topo_changed = True

                # Send KEEP_ALIVE to alive neighbors
                for nid in nb_alive:
                    if nb_alive[nid]:
                        if failed_neighbor is not None and nid == failed_neighbor:
                            continue
                        if nb_addrs.get(nid) is not None:
                            try:
                                sock.sendto(f"{my_id} KEEP_ALIVE".encode(), nb_addrs[nid])
                            except (ConnectionResetError, OSError):
                                pass
                # Send Topology Update to Controller
                send_topo_update()

    # ============================================

    # Wait for Register Response
    while True:
        try:
            data, addr = sock.recvfrom(4096)
        except ConnectionResetError:
            continue
        lines = data.decode().strip().split('\n')
        if lines[0] == "REGISTER_RESPONSE":
            register_response_received()
            num_nb = int(lines[1])
            now = time.time()
            for i in range(num_nb):
                parts = lines[2 + i].split()
                nid = int(parts[0])
                is_alive_flag = parts[1] == "True"
                if is_alive_flag and len(parts) >= 4:
                    nb_addrs[nid] = (parts[2], int(parts[3]))
                    nb_alive[nid] = True
                else:
                    nb_addrs[nid] = None
                    nb_alive[nid] = False
                nb_last_ka[nid] = now
            break
    
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