import socket
import threading
import pickle
import os

from redis_store import PyRedisStore

# Simple Protocol: Commands are space-separated, newline terminated.
# Examples:
# SET mykey myvalue\n
# GET mykey\n
# LPUSH mylist val1 val2\n

# Response Format:
# Simple strings for OK/errors, prefixed length for bulk strings, etc.
# For simplicity, I am just sending back the string representation of the result.
# A real Redis uses the RESP protocol (REdis Serialization Protocol).

DEFAULT_PORT = 6380 # Using a different port than default Redis (6379)
DEFAULT_HOST = '127.0.0.1' # Listen only on localhost by default
DUMP_FILENAME = "pyredis_dump.pkl"

store = PyRedisStore()
store_lock = threading.Lock()

def load_data_from_disk():
    """Loads data from the pickle dump file if it exists."""
    global store
    if os.path.exists(DUMP_FILENAME):
        print(f"[Server] Found dump file '{DUMP_FILENAME}'. Loading data...")
        try:
            with open(DUMP_FILENAME, 'rb') as f:
                with store_lock:
                     loaded_data = pickle.load(f)
                     if isinstance(loaded_data, tuple) and len(loaded_data) == 2 and isinstance(loaded_data[0], dict) and isinstance(loaded_data[1], dict):
                         store._data = loaded_data[0]
                         store._expirations = loaded_data[1]
                         print(f"[Server] Successfully loaded {len(store._data)} keys from disk.")
                         keys_to_delete = []
                         for key in list(store._data.keys()):
                             if store._check_expiry(key):
                                 pass
                         print("[Server] Performed initial expiry check on loaded data.")
                     else:
                         print("[Server] Error: Dump file format is incorrect. Starting with empty store.")
                         store = PyRedisStore()
        except (pickle.UnpicklingError, EOFError, TypeError, Exception) as e:
            print(f"[Server] Error loading data from '{DUMP_FILENAME}': {e}. Starting with empty store.")
            store = PyRedisStore()
    else:
        print(f"[Server] Dump file '{DUMP_FILENAME}' not found. Starting with empty store.")


def save_data_to_disk():
    """Saves the current data and expirations to the pickle dump file."""
    print(f"[Server] Attempting to save data to '{DUMP_FILENAME}'...")
    with store_lock:
        try:
            data_to_save = (store._data, store._expirations)
            with open(DUMP_FILENAME, 'wb') as f:
                pickle.dump(data_to_save, f)
            print(f"[Server] Data successfully saved to '{DUMP_FILENAME}'.")
            return "OK"
        except Exception as e:
            print(f"[Server] Error saving data to '{DUMP_FILENAME}': {e}")
            return f"ERROR: Could not save data: {e}"

def handle_connection(conn, addr):
    """Handles a single client connection."""
    print(f"[Server] Connection accepted from {addr}")
    buffer = ""
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                print(f"[Server] Connection closed by {addr}")
                break

            buffer += data.decode('utf-8')

            while '\n' in buffer:
                command_line, buffer = buffer.split('\n', 1)
                command_line = command_line.strip()
                if not command_line: continue

                print(f"[Server] Received from {addr}: {command_line}")
                parts = command_line.split()
                if not parts: continue

                command = parts[0].upper()
                args = parts[1:]
                response = None

                if command == "SAVE":
                    if not args:
                        response = save_data_to_disk()
                    else:
                        response = "ERROR: 'save' command takes no arguments"
                else:
                    with store_lock:
                        try:
                            if command == "SET":
                                if len(args) == 2:
                                    response = store.command_set(args[0], args[1])
                                elif len(args) == 4 and args[2].upper() == "EX":
                                    response = store.command_set(args[0], args[1], expire_ms=args[3])
                                else:
                                    response = "ERROR: wrong number of arguments for 'set' command"
                            elif command == "GET":
                                if len(args) == 1:
                                    response = store.command_get(args[0])
                                elif command == "DEL":
                                     if len(args) >= 1: response = store.command_del(*args)
                                     else: response = "ERROR: wrong number of arguments for 'del' command"
                                elif command == "LPUSH":
                                    if len(args) >= 2: response = store.command_lpush(args[0], *args[1:])
                                    else: response = "ERROR: wrong number of arguments for 'lpush' command"
                                elif command == "RPUSH":
                                    if len(args) >= 2: response = store.command_rpush(args[0], *args[1:])
                                    else: response = "ERROR: wrong number of arguments for 'rpush' command"
                                elif command == "LRANGE":
                                    if len(args) == 3: response = store.command_lrange(args[0], args[1], args[2])
                                    else: response = "ERROR: wrong number of arguments for 'lrange' command"
                                elif command == "HSET":
                                    if len(args) == 3: response = store.command_hset(args[0], args[1], args[2])
                                    else: response = "ERROR: wrong number of arguments for 'hset' command"
                                elif command == "HGET":
                                    if len(args) == 2: response = store.command_hget(args[0], args[1])
                                    else: response = "ERROR: wrong number of arguments for 'hget' command"
                                elif command == "HDEL":
                                    if len(args) >= 2: response = store.command_hdel(args[0], *args[1:])
                                    else: response = "ERROR: wrong number of arguments for 'hdel' command"
                                elif command == "TTL":
                                     if len(args) == 1: response = store.command_ttl(args[0])
                                     else: response = "ERROR: wrong number of arguments for 'ttl' command"
                                elif command == "EXPIRE":
                                     if len(args) == 2: response = store.command_expire(args[0], args[1])
                                     else: response = "ERROR: wrong number of arguments for 'expire' command"
                                elif command == "PING": response = "PONG"
                                elif command == "COMMAND": response = "Commands: ..."
                                elif command == "QUIT": response = "OK"
                                else: response = f"ERROR: Unknown command '{command}'"

                        except Exception as e:
                            print(f"[Server] Error executing command '{command_line}': {e}")
                            response = f"ERROR: Internal server error: {e}"

                if command == "QUIT":
                     response_str = "OK"
                     print(f"[Server] Sending to {addr}: {response_str}")
                     conn.sendall(f"{response_str}\n".encode('utf-8'))
                     print(f"[Server] QUIT received, closing connection to {addr}")
                     conn.close()
                     return

                if response is None: response_str = "Nil"
                elif isinstance(response, list): response_str = "\n".join(map(str, response))
                elif isinstance(response, int): response_str = f":{response}"
                else: response_str = str(response)

                print(f"[Server] Sending to {addr}: {response_str[:100]}...")
                conn.sendall(f"{response_str}\n".encode('utf-8'))

    except ConnectionResetError:
        print(f"[Server] Connection reset by peer {addr}")
    except Exception as e:
        print(f"[Server] Error handling connection from {addr}: {e}")
    finally:
        print(f"[Server] Cleaning up connection for {addr}")
        conn.close()

def run_server(host=DEFAULT_HOST, port=DEFAULT_PORT):
    """Loads data, then starts the PyRedis server."""
    load_data_from_disk()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"[Server] PyRedis server listening on {host}:{port}")

        while True:
            conn, addr = server_socket.accept()
            thread = threading.Thread(target=handle_connection, args=(conn, addr))
            thread.daemon = True
            thread.start()

    except OSError as e:
        print(f"[Server] Error binding to {host}:{port} - {e}")
    except KeyboardInterrupt:
        print("\n[Server] Shutting down server...")
    finally:
        print("[Server] Closing server socket.")
        server_socket.close()

if __name__ == "__main__":
    run_server()
