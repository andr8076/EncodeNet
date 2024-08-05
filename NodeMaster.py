import socket
import threading
import queue
import json
from tqdm import tqdm
import os
import time
import random

# ASCII Art Placeholder
print("ASCII ART HERE")

# Greek mythological names
names = ["Zeus", "Hera", "Poseidon", "Demeter", "Athena", "Apollo", "Artemis", "Ares", "Aphrodite", "Hermes"]
random.shuffle(names)

# Function to check if a NodeSlave is ready and assign a unique name
def check_slave_ready_and_assign_name(slave_ip, slave_port, name):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((slave_ip, slave_port))
            s.sendall(f"READY? {name}".encode('utf-8'))
            response = s.recv(1024).decode()
            if response == "READY":
                return True
    except Exception as e:
        print(f"Error checking readiness of NodeSlave {slave_ip}: {e}")
    return False

# Configuration prompts
server_port = int(input("Enter the server port for NodeMaster: "))
n_slaves = int(input("Enter the number of NodeSlave nodes: "))
slaves = []
slave_status = {}
for i in range(n_slaves):
    while True:
        ip = input(f"Enter IP address for NodeSlave {i+1}: ")
        port = int(input(f"Enter port for NodeSlave {i+1}: "))
        name = names.pop()
        if check_slave_ready_and_assign_name(ip, port, name):
            slaves.append((ip, port, name))
            slave_status[(ip, port)] = {"status": "Ready", "name": name}
            break
        else:
            print(f"NodeSlave {ip}:{port} is not ready. Please check the IP address and port and try again.")

# Task queue
task_queue = queue.Queue()

# Function to update and display the status of each NodeSlave
def display_status():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("Current NodeSlave Status:")
    for slave, status_info in slave_status.items():
        print(f"NodeSlave {status_info['name']} ({slave[0]}:{slave[1]}) - {status_info['status']}")

# Function to handle video encoding task by a NodeSlave
def handle_task(slave_ip, slave_port, name):
    while not task_queue.empty():
        task_data = task_queue.get()
        if not check_slave_ready_and_assign_name(slave_ip, slave_port, name):
            print(f"NodeSlave {name} not ready, re-queuing the task.")
            task_queue.put(task_data)
            continue
        try:
            slave_status[(slave_ip, slave_port)]["status"] = "Sending task"
            display_status()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((slave_ip, slave_port))
                s.sendall(json.dumps(task_data).encode('utf-8'))
                slave_status[(slave_ip, slave_port)]["status"] = "Encoding"
                display_status()
                response = s.recv(1024).decode('utf-8')
                print(f"Response from NodeSlave {name}: {response}")
                slave_status[(slave_ip, slave_port)]["status"] = "Ready"
                display_status()
        except Exception as e:
            print(f"Error communicating with NodeSlave {name}: {e}")
            task_queue.put(task_data)  # Re-queue the task if there's an error
            slave_status[(slave_ip, slave_port)]["status"] = "Error"
            display_status()
        finally:
            task_queue.task_done()

# Main function to start NodeMaster
def main():
    # Start worker threads for each NodeSlave
    threads = []
    for slave_ip, slave_port, name in slaves:
        t = threading.Thread(target=handle_task, args=(slave_ip, slave_port, name))
        t.start()
        threads.append(t)
    
    # Display status periodically
    while any(t.is_alive() for t in threads):
        display_status()
        time.sleep(5)  # Update every 5 seconds
    
    # Wait for all threads to finish
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
