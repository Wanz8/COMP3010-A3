import hashlib
import json
import signal
import socket
import string
import sys
import threading
import time

import numpy as np


DIFFICULTY = 9

global is_exit
is_exit = False

global is_mining
is_mining = False

def exit_handler(signum, frame):
    global is_exit
    is_exit = True
    sys.exit(0)


signal.signal(signal.SIGINT, exit_handler)
signal.signal(signal.SIGTERM, exit_handler)


def generate_nonce():
    # Define the characters that can be used to generate the nonce
    characters = string.digits
    # Generate a random string of the specified length
    length = np.random.randint(8, 40)
    nonce = "".join(characters[np.random.choice(len(characters))] for _ in range(length))
    return nonce


class Miner:
    def __init__(self, host, port):
        self.last_hash = None
        self.words = []
        self.nonce = None
        self.connect(host, int(port))
        recv_thread = threading.Thread(target=self.recv)
        recv_thread.daemon = True
        recv_thread.start()
        while is_exit == False:
            time.sleep(10)
        self.sock.close()

    def connect(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        print("Connected to {}:{}".format(host, port))

    def recv(self):
        global is_mining
        while is_exit == False:
            try:
                data = self.sock.recv(1024)
                data = json.loads(data.decode())
                if data["type"] == "MINING":
                    if self.last_hash == data["last_hash"] and self.words == data["words"]:
                        continue
                    self.last_hash = data["last_hash"]
                    self.name = data["name"]
                    self.words = data["words"]
                elif data["type"] == "NEW_WORD":
                    self.words = data["word"]
                if is_mining == True:
                    is_mining = False
                    time.sleep(1)
                    self.mining_thread.join()
                is_mining = True 
                self.mining_thread = threading.Thread(target=self.mining)
                self.mining_thread.daemon = True        
                self.mining_thread.start()
            except Exception as e:
                print(data)
                print(e)
                print("Blockchain process disconnected")
                break

    def mining(self):
        print("Mine with last hash: {} and words: {}".format(self.last_hash, self.words))
        while is_mining:
            hashBase = hashlib.sha256()

            hashBase.update(self.last_hash.encode())

            # add the miner
            hashBase.update(self.name.encode())

            # add the messages in order
            for word in self.words:
                hashBase.update(word.encode())

            # add the timestamp
            timestamp = int(time.time())
            hashBase.update(timestamp.to_bytes(8, "big"))

            # add the nonce
            nonce = generate_nonce()
            hashBase.update(nonce.encode())

            # get the pretty hexadecimal
            hash = hashBase.hexdigest()
            # is it difficult enough? Do I have enough zeros?
            if hash[-1 * DIFFICULTY :] != "0" * DIFFICULTY:
                continue

            message = {
                "type": "MINED",
                "minedBy": self.name,
                "nonce": nonce,
                "messages": self.words,
                "timestamp": timestamp,
                "hash": hash,
            }

            print(message)
            self.sock.send(json.dumps(message).encode())


if __name__ == "__main__":
    miner = Miner("localhost", 8081)
