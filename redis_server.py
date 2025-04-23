import socket
import threading
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

store = PyRedisStore()
store_lock = threading.Lock()

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
                if not command_line:
                    continue

                print(f"[Server] Received from {addr}: {command_line}")
                parts = command_line.split()
                if not parts:
                    continue

                command = parts[0].upper()
                args = parts[1:]

                response = None
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
                            else:
                                response = "ERROR: wrong number of arguments for 'get' command"
                        elif command == "DEL":
                             if len(args) >= 1:
                                 response = store.command_del(*args)
                             else:
                                 response = "ERROR: wrong number of arguments for 'del' command"
                        elif command == "LPUSH":
                            if len(args) >= 2:
                                response = store.command_lpush(args[0], *args[1:])
                            else:
                                response = "ERROR: wrong number of arguments for 'lpush' command"
                        elif command == "RPUSH":
                            if len(args) >= 2:
                                response = store.command_rpush(args[0], *args[1:])
                            else:
                                response = "ERROR: wrong number of arguments for 'rpush' command"
                        elif command == "LRANGE":
                            if len(args) == 3:
                                response = store.command_lrange(args[0], args[1], args[2])
                            else:
                                response = "ERROR: wrong number of arguments for 'lrange' command"
                        elif command == "HSET":
                            if len(args) == 3:
                                response = store.command_hset(args[0], args[1], args[2])
                            else:
                                response = "ERROR: wrong number of arguments for 'hset' command"
                        elif command == "HGET":
                            if len(args) == 2:
                                response = store.command_hget(args[0], args[1])
                            else:
                                response = "ERROR: wrong number of arguments for 'hget' command"
                        elif command == "HDEL":
                            if len(args) >= 2:
                                response = store.command_hdel(args[0], *args[1:])
                            else:
                                response = "ERROR: wrong number of arguments for 'hdel' command"
                        elif command == "TTL":
                             if len(args) == 1:
                                 response = store.command_ttl(args[0])
                             else:
                                 response = "ERROR: wrong number of arguments for 'ttl' command"
                        elif command == "EXPIRE":
                             if len(args) == 2:
                                 response = store.command_expire(args[0], args[1])
                             else:
                                 response = "ERROR: wrong number of arguments for 'expire' command"
                        elif command == "PING":
                             response = "PONG"
                        elif command == "COMMAND":
                             response = "Commands: SET, GET, DEL, LPUSH, RPUSH, LRANGE, HSET, HGET, HDEL, TTL, EXPIRE, PING, COMMAND, QUIT"
                        elif command == "QUIT":
                             response = "OK"
                             conn.sendall(f"{response}\n".encode('utf-8'))
                             print(f"[Server] QUIT received, closing connection to {addr}")
                             conn.close()
                             return
                        else:
                            response = f"ERROR: Unknown command '{command}'"
                    except Exception as e:
                        print(f"[Server] Error executing command '{command_line}': {e}")
                        response = f"ERROR: Internal server error during command execution: {e}"

                if response is None:
                    response_str = "Nil"
                elif isinstance(response, list):
                    response_str = "\n".join(map(str, response))
                elif isinstance(response, int):
                    response_str = f":{response}"
                else:
                    response_str = str(response)

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
    """Starts the PyRedis server."""
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
        print(f"[Server] Error binding to {host}:{port} - {e}. Is the port already in use?")
    except KeyboardInterrupt:
        print("\n[Server] Shutting down server...")
    finally:
        print("[Server] Closing server socket.")
        server_socket.close()

if __name__ == "__main__":
    run_server()
