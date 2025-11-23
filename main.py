import asyncio
import json
import time
import os
from resp import serialize, deserialize

DICTIONARY = {}
EXPIRATION = {}
DB_FILE = "db.json"

async def save_db():
    db = {"data": DICTIONARY, "exp": EXPIRATION}
    with open(DB_FILE, "w") as f:
        json.dump(db, f)
    print("Database saved.")

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r") as f:
            db = json.load(f)
            DICTIONARY.update(db.get("data", {}))
            EXPIRATION.update(db.get("exp", {}))
        print("Database loaded.")

load_db()

async def expiration_task():
    while True:
        now = time.time()
        expired_keys = [k for k, t in EXPIRATION.items() if t <= now]
        for key in expired_keys:
            DICTIONARY.pop(key, None)
            EXPIRATION.pop(key, None)
        await asyncio.sleep(0.1)

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info('peername')
    print(f"Connected by {addr}")

    try:
        while True:
            raw = await reader.read(1024)
            if not raw:
                break

            data = deserialize(raw)
            if not isinstance(data, list) or len(data) == 0:
                writer.write(serialize("ERR invalid input", is_error=True))
                await writer.drain()
                continue

            cmd = data[0].upper()

            if cmd == "PING":
                writer.write(serialize("PONG"))

            elif cmd == "ECHO" and len(data) > 1:
                writer.write(serialize(data[1]))

            elif cmd == "SET" and len(data) > 2:
                key = data[1]
                value = data[2]
                timeout = None

                i = 3
                while i < len(data):
                    flag = data[i].upper()
                    if i + 1 >= len(data):
                        writer.write(serialize("ERR syntax error", is_error=True))
                        break
                    timeout_value = float(data[i + 1])
                    if flag == "EX":
                        timeout = timeout_value
                    elif flag == "PX":
                        timeout = timeout_value / 1000
                    elif flag == "EXAT":
                        timeout = timeout_value - time.time()
                    elif flag == "PXAT":
                        timeout = timeout_value / 1000 - time.time()
                    else:
                        writer.write(serialize("ERR syntax error", is_error=True))
                        break
                    i += 2

                DICTIONARY[key] = value
                if timeout is not None:
                    EXPIRATION[key] = time.time() + timeout
                writer.write(serialize("OK"))

            elif cmd == "GET" and len(data) > 1:
                key = data[1]
                if key in EXPIRATION and time.time() >= EXPIRATION[key]:
                    DICTIONARY.pop(key, None)
                    EXPIRATION.pop(key, None)
                if key in DICTIONARY:
                    writer.write(serialize(DICTIONARY[key]))
                else:
                    writer.write(serialize("ERR Key not set!", is_error=True))

            elif cmd == "DEL" and len(data) > 1:
                deleted = 0
                for key in data[1:]:
                    if DICTIONARY.pop(key, None) is not None:
                        deleted += 1
                        EXPIRATION.pop(key, None)
                writer.write(serialize(deleted))

            elif cmd == "EXISTS" and len(data) > 1:
                count = sum(1 for key in data[1:] if key in DICTIONARY)
                writer.write(serialize(count))

            elif cmd == "INCR" and len(data) > 1:
                key = data[1]
                val = DICTIONARY.get(key, "0")
                try:
                    val = int(val) + 1
                    DICTIONARY[key] = val
                    writer.write(serialize(val))
                except ValueError:
                    writer.write(serialize("ERR value is not an integer", is_error=True))

            elif cmd == "DECR" and len(data) > 1:
                key = data[1]
                val = DICTIONARY.get(key, "0")
                try:
                    val = int(val) - 1
                    DICTIONARY[key] = val
                    writer.write(serialize(val))
                except ValueError:
                    writer.write(serialize("ERR value is not an integer", is_error=True))

            elif cmd == "LPUSH" and len(data) > 2:
                key = data[1]
                values = data[2:]
                lst = DICTIONARY.get(key, [])
                if not isinstance(lst, list):
                    lst = []
                DICTIONARY[key] = values + lst
                writer.write(serialize(len(DICTIONARY[key])))

            elif cmd == "RPUSH" and len(data) > 2:
                key = data[1]
                values = data[2:]
                lst = DICTIONARY.get(key, [])
                if not isinstance(lst, list):
                    lst = []
                DICTIONARY[key] = lst + values
                writer.write(serialize(len(DICTIONARY[key])))

            elif cmd == "SAVE":
                await save_db()
                writer.write(serialize("OK"))

            else:
                writer.write(serialize("ERR unknown command", is_error=True))

            await writer.drain()

    except ConnectionResetError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Connection closed: {addr}")

async def main():
    HOST = "0.0.0.0"
    PORT = 6379

    server = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"Listening on {HOST}:{PORT}")

    asyncio.create_task(expiration_task())

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
