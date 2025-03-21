import time
import socket

import requests

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 22250     # Port to listen on (non-privileged ports are > 1023)

start=time.time()
sec=360000
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print('Connected by', addr)
        while time.time() < start + sec:
            data = requests.get('http://api.open-notify.org/iss-now.json').text
            conn.sendall(str.encode(data+'\n'))
            time.sleep(5)