import hashlib
import signal
import sys
import socket
import time
import uuid
import json
import threading
from datetime import datetime

import numpy as np

MTU = 1500
repeat = 3
heartbeat_interval = 15
DIFFICULTY = 9

global is_exit
is_exit = False


def exit_handler(signum, frame):
    global is_exit
    is_exit = True
    sys.exit(0)


signal.signal(signal.SIGINT, exit_handler)
signal.signal(signal.SIGTERM, exit_handler)


def verify_new_block(blockchain, new_block):
    hashBase = hashlib.sha256()
    if len(blockchain) != 0 and blockchain[-1]["hash"] != None:
        hashBase = hashlib.sha256()
        # get the most recent hash
        lastHash = blockchain[-1]["hash"]

        # add it to this hash
        hashBase.update(lastHash.encode())

    if (
        new_block == None
        or new_block["minedBy"] == None
        or new_block["minedBy"] == "None"
        or new_block["hash"] == None
        or new_block["hash"] == "None"
        or "messages" not in new_block
        or "timestamp" not in new_block
        or "nonce" not in new_block
    ):
        return False

    # add the miner
    hashBase.update(new_block["minedBy"].encode())

    # add the messages in order
    for m in new_block["messages"]:
        hashBase.update(m.encode())

    # add the timestamp
    new_block["timestamp"] = int(new_block["timestamp"])
    hashBase.update(new_block["timestamp"].to_bytes(8, "big"))

    # add the nonce
    hashBase.update(new_block["nonce"].encode())

    # get the pretty hexadecimal
    hash = hashBase.hexdigest()

    # is it difficult enough? Do I have enough zeros?
    if hash[-1 * DIFFICULTY :] != "0" * DIFFICULTY:
        print("Block was not difficult enough: {}".format(hash))
        return False
    if hash != new_block["hash"]:
        print("Block hash was incorrect: {}".format(hash))
        return False
    return True


class Peer:
    def __init__(self, server_addr, miner_number=0):
        self.messages_repeated = []
        self.peers = {}
        self.name = "Anonymous"
        self.server_addr = server_addr
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addr = self.get_localhost_addr()
        self.sock.bind(self.addr)
        print("Peer started at {}:{}".format(self.addr[0], self.addr[1]))
        self.blockchain = []
        self.words = ["Mined by Anonymous"]
        self.miners = []
        self.consensusing = False
        if miner_number > 0:
            wait_miner_thread = threading.Thread(
                target=self.wait_miner, args=(miner_number,)
            )
            wait_miner_thread.daemon = True
            wait_miner_thread.start()

    def get_localhost_addr(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("130.179.28.37", 80))
            addr = s.getsockname()
        finally:
            s.close()
        addr = (addr[0], 8911)
        return addr

    def wait_miner(self, miner_number):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("localhost", 8081))
        sock.listen(miner_number)
        while True:
            conn, addr = sock.accept()
            self.miners.append(conn)
            print("Connected to miner {}".format(addr))
            msg = {
                "type": "MINING",
                "name": self.name,
                "last_hash": self.blockchain[-1]["hash"]
                if len(self.blockchain) > 0
                else None,
                "words": self.words if len(self.words) else None,
            }
            conn.send(json.dumps(msg).encode())
            mining_thread = threading.Thread(target=self.mining, args=(conn,))
            mining_thread.daemon = True
            mining_thread.start()

    def mining(self, miner_conn):
        while True:
            try:
                data = miner_conn.recv(1024)
                data = json.loads(data.decode())
                print("Received message from miner:{}".format(data))
                if data["type"] == "MINED":
                    message = {
                        "type": "ANNOUNCE",
                        "height": len(self.blockchain),
                        "minedBy": self.name,
                        "nonce": data["nonce"],
                        "messages": data["messages"],
                        "timestamp": data["timestamp"],
                        "hash": data["hash"],
                    }
                    if verify_new_block(self.blockchain, message):
                        self.blockchain.append(message)
                        print(
                            "Added block {} to blockchain".format(len(self.blockchain))
                        )
                        for peer_addr in self.peers:
                            self.send(json.dumps(message), peer_addr)
                        self.words = ["Mined by {}".format(self.name)]
                        for miner in self.miners:
                            message = {
                                "type": "MINING",
                                "name": self.name,
                                "last_hash": self.blockchain[-1]["hash"]
                                if len(self.blockchain) > 0
                                else None,
                                "words": self.words if len(self.words) else None,
                            }
                            miner.send(json.dumps(message).encode())
            except Exception as e:
                print("Miner disconnected")
                self.miners.remove(miner_conn)
                return

    def start(self):
        self.gossip()
        recv_thread = threading.Thread(target=self.recv)
        recv_thread.daemon = True
        recv_thread.start()

        heartbeat_thread = threading.Thread(target=self.heartbeat)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        consensus_thread = threading.Thread(target=self.consensus)
        consensus_thread.daemon = True
        consensus_thread.start()

        global is_exit
        while is_exit == False:
            time.sleep(30)
        self.sock.close()

    def gossip(self):
        msg = {
            "type": "GOSSIP",
            "host": self.addr[0],
            "port": self.addr[1],
            "id": str(uuid.uuid4()),
            "name": self.name,
        }
        self.messages_repeated.append(msg["id"])
        self.send(json.dumps(msg), self.server_addr)

    def consensus(self):
        while is_exit == False:
            time.sleep(30)
            self.do_consensus()

    def do_consensus(self):
        if self.consensusing:
            return
        self.consensusing = True
        try:
            highest_height = -1
            hash_count = {}
            peer_blockchains = {}
            for peer_addr in self.peers:
                msg = {
                    "type": "STATS",
                }
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(2)
                print("Sending message:{}".format(msg))
                sock.sendto(json.dumps(msg).encode(), peer_addr)
                try:
                    data, addr = sock.recvfrom(MTU)
                except socket.timeout:
                    print(f"Timeout when doing consensus.")
                    continue
                data = json.loads(data.decode())
                print("Received message:{}".format(data))
                sock.close()
                if "height" not in data or "hash" not in data:
                    continue
                if data["height"] == None:
                    continue
                if (
                    data["hash"] == None
                    or data["hash"][-1 * DIFFICULTY :] != "0" * DIFFICULTY
                ):
                    continue
                peer_blockchains[peer_addr] = data
                if data["height"] > highest_height:
                    highest_height = data["height"]
                    hash_count = {data["hash"]: 1}
                elif data["height"] == highest_height:
                    if data["hash"] in hash_count:
                        hash_count[data["hash"]] += 1
                    else:
                        hash_count[data["hash"]] = 1

            if len(peer_blockchains) == 0:
                return

            most_common_hash = max(hash_count, key=hash_count.get)
            peers_with_most_agreed_blockchain = [
                peer_addr
                for peer_addr, data in peer_blockchains.items()
                if data["hash"] == most_common_hash
            ]
            if len(self.blockchain) > 0:
                if (
                    highest_height == self.blockchain[-1]["height"]
                    and most_common_hash == self.blockchain[-1]["hash"]
                ):
                    self.consensusing = False
                    return
            while len(self.blockchain) > 0:
                msg = {
                    "type": "GET_BLOCK",
                    "height": self.blockchain[-1]["height"],
                }
                for peer_addr in peers_with_most_agreed_blockchain:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.settimeout(5)
                    for _ in range(2):  # Try twice
                        try:
                            print("Sending message:{}".format(msg))
                            sock.sendto(json.dumps(msg).encode(), peer_addr)
                            data, addr = sock.recvfrom(MTU)
                            print(
                                "Received message:{}".format(json.loads(data.decode()))
                            )
                            break  # If successful, break the inner loop
                        except socket.timeout:
                            print(f"Timeout when receiving from {peer_addr}, retrying.")
                    else:  # If we've tried twice and still failed, move to next peer
                        print(
                            f"Failed to receive from {peer_addr} after 2 attempts, moving to next peer."
                        )
                        continue
                peer_block = json.loads(data.decode())
                if "height" not in peer_block or "hash" not in peer_block:
                    continue
                sock.close()
                if (
                    peer_block["height"] == self.blockchain[-1]["height"]
                    and peer_block["hash"] == self.blockchain[-1]["hash"]
                ):
                    break
                self.blockchain.pop()
            for height in range(len(self.blockchain), highest_height + 1):
                msg = {
                    "type": "GET_BLOCK",
                    "height": height,
                }
                for peer_addr in peers_with_most_agreed_blockchain:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.settimeout(5)
                    for _ in range(2):  # Try twice
                        try:
                            print("Sending message:{}".format(msg))
                            sock.sendto(json.dumps(msg).encode(), peer_addr)
                            data, addr = sock.recvfrom(MTU)
                            print(
                                "Received message:{}".format(json.loads(data.decode()))
                            )
                            break  # If successful, break the inner loop
                        except socket.timeout:
                            print(f"Timeout when receiving from {peer_addr}, retrying.")
                    else:  # If we've tried twice and still failed, move to next peer
                        print(
                            f"Failed to receive from {peer_addr} after 2 attempts, moving to next peer."
                        )
                        continue
                    new_block = json.loads(data.decode())
                    sock.close()
                    if not verify_new_block(self.blockchain, new_block):
                        continue
                    self.blockchain.append(new_block)
                    print("Added block {} to blockchain".format(height))
                    break
                if len(self.blockchain) != height + 1:
                    print("Failed to reach consensus on block {}".format(height))
                    break
        except Exception as e:
            print(e)
            print("Consensus Finished!")
        self.consensusing = False
        for miner in self.miners:
            message = {
                "type": "MINING",
                "name": self.name,
                "last_hash": self.blockchain[-1]["hash"]
                if len(self.blockchain) > 0
                else None,
                "words": self.words if len(self.words) else None,
            }
            miner.send(json.dumps(message).encode())

    def send(self, msg, addr):
        if len(msg) > MTU:
            raise ValueError("Message too long")
        try:
            self.sock.sendto(msg.encode(), addr)
        except Exception as e:
            print("Sending message:{} failed when send to {}".format(msg, addr))
            # Close the current socket
            self.sock.close()
            # Create a new socket
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(self.addr)


    def handle_gossip(self, data):
        peer_addr = (data["host"], data["port"])
        message_id = data["id"]
        if message_id in self.messages_repeated:
            return
        if len(self.peers) < repeat:
            forward_to = np.random.choice(
                len(self.peers), len(self.peers), replace=False
            )
        else:
            forward_to = np.random.choice(len(self.peers), repeat, replace=False)
        for i in forward_to:
            self.send(json.dumps(data), list(self.peers.keys())[i])
        now = datetime.now()
        self.peers[peer_addr] = now
        response = {
            "type": "GOSSIP_REPLY",
            "host": self.addr[0],
            "port": self.addr[1],
            "name": self.name,
        }
        self.messages_repeated.append(message_id)
        self.send(json.dumps(response), peer_addr)

    def handle_gossip_reply(self, data):
        peer_addr = (data["host"], data["port"])
        now = datetime.now()
        self.peers[peer_addr] = now

    def handle_stats(self, data, addr):
        if len(self.blockchain) == 0:
            msg = {
                "type": "STATS_REPLY",
                "height": None,
                "hash": None,
            }
        else:
            msg = {
                "type": "STATS_REPLY",
                "height": len(self.blockchain),
                "hash": self.blockchain[-1]["hash"],
            }
        self.send(json.dumps(msg), addr)

    def handle_get_block(self, data, addr):
        height = data["height"]
        if height >= len(self.blockchain):
            msg = {
                "type": "GET_BLOCK_REPLY",
                "height": None,
                "minedBy": None,
                "nonce": None,
                "messages": None,
                "timestamp": None,
                "hash": None,
            }
        else:
            msg = {
                "type": "GET_BLOCK_REPLY",
                "height": self.blockchain[height]["height"],
                "minedBy": self.blockchain[height]["minedBy"],
                "nonce": self.blockchain[height]["nonce"],
                "messages": self.blockchain[height]["messages"],
                "timestamp": self.blockchain[height]["timestamp"],
                "hash": self.blockchain[height]["hash"],
            }
        self.send(json.dumps(msg), addr)

    def handle_new_word(self, data):
        print("Received new word: {}".format(data["word"]))
        self.words.append(data["word"])
        for miner in self.miners:
            message = {
                "type": "NEW_WORD",
                "word": self.words,
            }
            miner.send(json.dumps(message).encode())

    def handle_announce(self, data):
        chunk = {}
        chunk["height"] = data["height"]
        chunk["minedBy"] = data["minedBy"]
        chunk["nonce"] = data["nonce"]
        chunk["messages"] = data["messages"]
        chunk["timestamp"] = data["timestamp"]
        chunk["hash"] = data["hash"]
        if verify_new_block(self.blockchain, chunk):
            self.blockchain.append(chunk)
            print("Added block {} to blockchain".format(len(self.blockchain)))
            for miner in self.miners:
                message = {
                    "type": "MINING",
                    "name": self.name,
                    "last_hash": self.blockchain[-1]["hash"]
                    if len(self.blockchain) > 0
                    else None,
                    "words": self.words if len(self.words) else None,
                }
                miner.send(json.dumps(message).encode())

    def heartbeat(self):
        global is_exit
        while is_exit == False:
            delete = []
            for peer_addr, last_seen in self.peers.items():
                if (
                    datetime.now() - last_seen
                ).total_seconds() < heartbeat_interval * 10:
                    msg = {
                        "type": "GOSSIP",
                        "host": self.addr[0],
                        "port": self.addr[1],
                        "id": str(uuid.uuid4()),
                        "name": self.name,
                    }
                    self.messages_repeated.append(msg["id"])
                    self.send(json.dumps(msg), peer_addr)
                else:
                    delete.append(peer_addr)
            while self.consensusing == True:
                time.sleep(3)
            for peer_addr in delete:
                del self.peers[peer_addr]
            time.sleep(heartbeat_interval)

    def recv(self):
        global is_exit
        while is_exit == False:
            try:
                data, addr = self.sock.recvfrom(MTU)
            except Exception as e:
                print(e)
                continue
            data = json.loads(data.decode())
            try:
                if data["type"] == "GOSSIP":
                    self.handle_gossip(data)
                elif data["type"] == "GOSSIP_REPLY":
                    self.handle_gossip_reply(data)
                elif data["type"] == "CONSENSUS":
                    self.do_consensus()
                elif data["type"] == "STATS":
                    self.handle_stats(data, addr)
                elif data["type"] == "GET_BLOCK":
                    self.handle_get_block(data, addr)
                elif data["type"] == "NEW_WORD":
                    self.handle_new_word(data)
                elif data["type"] == "ANNOUNCE":
                    self.handle_announce(data)
                else:
                    print("Unknown message type:", data["type"])
            except Exception as e:
                print(e)
                print("Received invalid message:", data)
                continue


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python peer.py <host> <port>")
        sys.exit(1)

    host = sys.argv[1]
    port = sys.argv[2]
    server_addr = (host, int(port))

    peer = Peer(server_addr, 100)
    peer.start()
