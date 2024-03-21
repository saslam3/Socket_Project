import socket
import threading
import random

class Node:
    def __init__(self, key):
        self.left = None
        self.right = None
        self.key = key

class BinarySearchTree:
    def __init__(self):
        self.root = None

    def insert(self, root, key):
        if not root:
            return Node(key)
        if key < root.key:
            root.left = self.insert(root.left, key)
        elif key > root.key:
            root.right = self.insert(root.right, key)
        return root

    def search(self, root, key):
        if not root or root.key == key:
            return root
        if key < root.key:
            return self.search(root.left, key)
        return self.search(root.right, key)

    def delete(self, root, key):
        if not root:
            return root
        if key < root.key:
            root.left = self.delete(root.left, key)
        elif key > root.key:
            root.right = self.delete(root.right, key)
        else:
            if not root.left:
                return root.right
            elif not root.right:
                return root.left
            root.key = self.min_value_node(root.right).key
            root.right = self.delete(root.right, root.key)
        return root

    def min_value_node(self, node):
        current = node
        while current.left:
            current = current.left
        return current

class Peer:
    def __init__(self, name, ipv4_address, m_port, p_port):
        self.name = name
        self.ipv4_address = ipv4_address
        self.m_port = m_port
        self.p_port = p_port
        self.state = "Free"
        self.right_neighbor = None

class Manager:
    def __init__(self, port):
        self.port = port
        self.peers = {}
        self.peer_sockets = {}
        self.peer_address = {}
        self.dht_leader = None 
        self.dht_setup_in_progress = BinarySearchTree()
        self.dht_setup = False

    def handle_peer(self, peer_socket, peer_address):
        while True:
            data, addr = peer_socket.recvfrom(1024)
            if data:
                message = data.decode().split()
                command = message[0]
                peer_name = message[1]
                if command == "register":
                    self.register_peer(peer_name, message[2:])
                elif command == "setup-dht":
                    self.setup_dht(peer_name, message[2:])
                elif command == "leave-dht":
                    self.leave_dht(peer_name)
                elif command == "join-dht":
                    self.join_dht(peer_name)
                elif command == "teardown-dht":
                    self.teardown_dht(peer_name)
                elif command == "deregister":
                    self.deregister(peer_name)
                # Add handling for other commands
                else:
                    print("Invalid command.")

    def register_peer(self, peer):
        if peer.name not in self.peers:
            self.peers[peer.name] = peer
            return "SUCCESS"
        else:
            return "FAILURE"

    def setup_dht(self, leader_name, n, year):
        if leader_name not in self.peers:
            return "FAILURE"

        leader = self.peers[leader_name]

        if leader.state != "Free":
            return "FAILURE"

        if n < 3 or n > len(self.peers):
            return "FAILURE"

        if self.dht_setup_in_progress.search(self.dht_setup_in_progress.root, leader_name):
            return "FAILURE"

        self.dht_setup_in_progress.insert(self.dht_setup_in_progress.root, leader_name)

        dht_peers = [leader]
        free_peers = [peer for peer in self.peers.values() if peer.state == "Free" and peer != leader]

        if len(free_peers) < n - 1:
            return "FAILURE"

        dht_peers.extend(random.sample(free_peers, n - 1))

        for i in range(n - 1):
            dht_peers[i].right_neighbor = dht_peers[(i + 1) % n]

        leader.state = "Leader"
        for peer in dht_peers[1:]:
            peer.state = "InDHT"

        return self.dht_complete(leader_name)

    def dht_complete(self, leader_name):
        if not self.dht_setup_in_progress.search(self.dht_setup_in_progress.root, leader_name):
            return "FAILURE"

        leader = self.peers.get(leader_name)

        if not leader or leader.state != "Leader":
            return "FAILURE"

        self.dht_setup_in_progress.delete(self.dht_setup_in_progress.root, leader_name)

        return "SUCCESS"

    def leave_dht(self, leaving_peer):
        if self.dht_setup:
            # Perform DHT teardown
            self.dht_setup = False
            for peer_name, peer_info in self.peers.items():
                peer_socket = self.peer_sockets.get(peer_name)
                if peer_name != leaving_peer:
                    peer_socket.sendto("teardown".encode(), peer_info)
            # Perform renumbering of ring identifiers
            new_dht_peers = [(name, info) for name, info in self.dht_peers if name != leaving_peer]
            self.dht_peers = new_dht_peers
            self.dht_size -= 1
            response = "SUCCESS"
        else:
            response = "FAILURE"
        leaving_peer_socket = self.peer_sockets.get(leaving_peer)
        if leaving_peer_socket:
            leaving_peer_socket.sendto(response.encode(), self.peer_address)

    def join_dht(self, joining_peer):
        if self.dht_setup:
            # Add joining peer to the DHT
            joining_peer_info = self.peers.get(joining_peer)
            if joining_peer_info:
                self.dht_peers.append((joining_peer, *joining_peer_info))
                self.dht_size += 1
                response = "SUCCESS"
            else:
                response = "FAILURE"
        else:
            response = "FAILURE"
        joining_peer_socket = self.peer_sockets.get(joining_peer)
        joining_peer_address = self.peer_address.get(joining_peer)
        if joining_peer_socket and joining_peer_address:
            joining_peer_socket.sendto(response.encode(), joining_peer_address)


    def teardown_dht(self, leader_name):
        if self.dht_setup and self.dht_leader == leader_name:
            leader_info = self.peers.get(leader_name)
            leader_socket = self.peer_sockets.get(leader_name)  # Get the leader socket
            if leader_socket:
                for peer_name, peer_info in self.peers.items():
                    peer_socket = self.peer_sockets.get(peer_name)
                    peer_socket.sendto("teardown".encode(), peer_info)
                self.dht_setup = False
                self.dht_peers = []
                self.dht_leader = None
                self.dht_size = 0
                response = "SUCCESS"
            else:
                response = "FAILURE: Leader socket not found"
        else:
            response = "FAILURE: DHT teardown failed"
        return response


    def deregister(self, peer_name):
        if peer_name in self.peers:
            if peer_name == self.dht_leader:
                self.teardown_dht(peer_name)
            del self.peers[peer_name]
            response = "SUCCESS"
        else:
            response = "FAILURE"
        peer_socket = self.peer_sockets.get(peer_name)
        peer_address = self.peer_address.get(peer_name)
        peer_socket.sendto(response.encode(), peer_address)

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind(("localhost", self.port))

        while True:
            data, addr = server_socket.recvfrom(1024)

    def handle_query(self, peer_name, event_id):
        if not self.dht_setup:
            return "FAILURE: DHT not set up."

        if peer_name not in self.peers:
            return "FAILURE: Peer not registered."

        if self.peers[peer_name].state != "InDHT":
            return "FAILURE: Peer not in DHT."

        # Find the appropriate node in the DHT to handle the query
        query_node = self.find_query_node(event_id)

        if query_node:
            # Forward the query to the appropriate node
            query_result = self.forward_query(peer_name, query_node, event_id)
            return query_result
        else:
            return "FAILURE: Unable to find node to handle query."


if __name__ == "__main__":
    manager = Manager(port=44000)
    peer1 = Peer(name="Peer1", ipv4_address="127.0.0.1", m_port=44001, p_port=44002)
    peer2 = Peer(name="Peer2", ipv4_address="127.0.0.1", m_port=44003, p_port=44004)
    peer3 = Peer(name="Peer3", ipv4_address="127.0.0.1", m_port=44005, p_port=44006)
    

    manager_thread = threading.Thread(target=manager.start)
    manager_thread.start()

    # Register peers
    print(manager.register_peer(peer1))
    print(manager.register_peer(peer2))
    print(manager.register_peer(peer3))

    # Building DHT
    # Assuming "Peer1" is chosen as the leader to build the DHT
    print(manager.setup_dht("Peer1", 5, 1996))

    # Querying DHT
    event_ids = [5536849, 2402920, 5539287, 55770111]
    for event_id in event_ids:
        # Assuming manager handles query requests directly
        print(manager.handle_query("Peer2", event_id))
        print(manager.handle_query("Peer3", event_id))
        # Add similar commands for other peers if necessary

    # Leaving DHT
    # Assuming "Peer2" is leaving the DHT
    print(manager.leave_dht("Peer2"))

    # Joining DHT
    # Assuming "Peer4" is joining the DHT
    print(manager.join_dht("Peer4"))

    # Teardown DHT
    # Assuming the leader "Peer1" issues teardown-dht command
    print(manager.teardown_dht("Peer1"))

    # Graceful Termination
    # Assuming all peers de-register and exit
    print(manager.deregister("Peer1"))
    print(manager.deregister("Peer3"))
