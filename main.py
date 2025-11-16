from resp import serialize, deserialize
import threading
import time
import socket
import json
import os

DICTIONARY = {}         
EXPIRATION = {}          
lock = threading.Lock()  
DB_FILE = "db.json"      

def save_db():
    with lock:
        db = {
            "data": DICTIONARY,
            "exp": EXPIRATION
        }
        with open(DB_FILE, "w") as f:
            json.dump(db, f)
    print("Database saved.")

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r") as f:
            db = json.load(f)
            with lock:
                DICTIONARY.update(db.get("data", {}))
                EXPIRATION.update(db.get("exp", {}))
        print("Database loaded.")

load_db()

def set_with_timeout(key, value, timeout=None):
    with lock:
        DICTIONARY[key] = value
        if timeout is not None:
            EXPIRATION[key] = time.time() + timeout
            threading.Timer(timeout, lambda: remove_key(key)).start()

def remove_key(key):
    with lock:
        DICTIONARY.pop(key, None)
        EXPIRATION.pop(key, None)

def handle_client(conn, addr):
    print(f"Connected by {addr}")
    while True:
        try:
            raw = conn.recv(1024)
            if not raw:
                break  

            data = deserialize(raw)
            print(f"Received: {data}")

            if not isinstance(data, list) or len(data) == 0:
                conn.sendall(serialize("ERR invalid input", is_error=True))
                continue

            cmd = data[0].upper()

            if cmd == "PING":
                conn.sendall(serialize("PONG"))

            elif cmd == "ECHO" and len(data) > 1:
                conn.sendall(serialize(data[1]))

            elif cmd == "SET" and len(data) > 2:
                key = data[1]
                value = data[2]
                timeout = None

                if len(data) > 3:
                    i = 3
                    while i < len(data):
                        flag = data[i].upper()
                        if i + 1 >= len(data):
                            conn.sendall(serialize("ERR syntax error", is_error=True))
                            break
                        timeout_value = float(data[i+1])
                        if flag == "EX":
                            timeout = timeout_value
                        elif flag == "PX":
                            timeout = timeout_value / 1000
                        elif flag == "EXAT":
                            timeout = timeout_value - time.time()
                        elif flag == "PXAT":
                            timeout = timeout_value / 1000 - time.time()
                        else:
                            conn.sendall(serialize("ERR syntax error", is_error=True))
                            break
                        i += 2

                set_with_timeout(key, value, timeout)
                conn.sendall(serialize("OK"))

            elif cmd == "GET" and len(data) > 1:
                key = data[1]
                with lock:
                    if key in EXPIRATION and time.time() >= EXPIRATION[key]:
                        remove_key(key)
                    if key in DICTIONARY:
                        conn.sendall(serialize(DICTIONARY[key]))
                    else:
                        conn.sendall(serialize("ERR Key not set!", is_error=True))

            elif cmd == "EXISTS" and len(data) > 1:
                count = 0
                with lock:
                    for key in data[1:]:
                        if key in DICTIONARY:
                            count += 1
                conn.sendall(serialize(count))

            elif cmd == "DEL" and len(data) > 1:
                deleted = 0
                with lock:
                    for key in data[1:]:
                        if DICTIONARY.pop(key, None) is not None:
                            deleted += 1
                            EXPIRATION.pop(key, None)
                conn.sendall(serialize(deleted))

            elif cmd == "INCR" and len(data) > 1:
                key = data[1]
                with lock:
                    val = DICTIONARY.get(key, "0")
                    try:
                        val = int(val) + 1
                        DICTIONARY[key] = val
                        conn.sendall(serialize(val))
                    except ValueError:
                        conn.sendall(serialize("ERR value is not an integer", is_error=True))

            elif cmd == "DECR" and len(data) > 1:
                key = data[1]
                with lock:
                    val = DICTIONARY.get(key, "0")
                    try:
                        val = int(val) - 1
                        DICTIONARY[key] = val
                        conn.sendall(serialize(val))
                    except ValueError:
                        conn.sendall(serialize("ERR value is not an integer", is_error=True))

            elif cmd == "LPUSH" and len(data) > 2:
                key = data[1]
                values = data[2:]
                with lock:
                    lst = DICTIONARY.get(key, [])
                    if not isinstance(lst, list):
                        lst = []
                    DICTIONARY[key] = values + lst
                    conn.sendall(serialize(len(DICTIONARY[key])))

            elif cmd == "RPUSH" and len(data) > 2:
                key = data[1]
                values = data[2:]
                with lock:
                    lst = DICTIONARY.get(key, [])
                    if not isinstance(lst, list):
                        lst = []
                    DICTIONARY[key] = lst + values
                    conn.sendall(serialize(len(DICTIONARY[key])))

            elif cmd == "SAVE":
                save_db()
                conn.sendall(serialize("OK"))

            else:
                conn.sendall(serialize("ERR unknown command", is_error=True))

        except ConnectionResetError:
            break
        except Exception as e:
            conn.sendall(serialize(f"ERR {e}", is_error=True))

    conn.close()
    print(f"Connection closed: {addr}")

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print(f"Listening on {HOST}:{PORT}")
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
