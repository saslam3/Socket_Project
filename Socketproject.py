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
        self.dht_setup_in_progress = BinarySearchTree()

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

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind(("localhost", self.port))

        while True:
            data, addr = server_socket.recvfrom(1024)

if __name__ == "__main__":
    manager = Manager(port=44000)
    peer1 = Peer(name="Peer1", ipv4_address="127.0.0.1", m_port=44001, p_port=44002)
    peer2 = Peer(name="Peer2", ipv4_address="127.0.0.1", m_port=44003, p_port=44004)
    peer3 = Peer(name="Peer3", ipv4_address="127.0.0.1", m_port=44005, p_port=44006)
    

    manager_thread = threading.Thread(target=manager.start)
    manager_thread.start()

    print(manager.register_peer(peer1))
    print(manager.register_peer(peer2))
    print(manager.register_peer(peer3))

    manager.setup_dht(leader_name="Peer1", n=3, year=2022)
