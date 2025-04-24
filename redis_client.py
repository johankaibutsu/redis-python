import socket
import sys

DEFAULT_PORT = 6380
DEFAULT_HOST = '127.0.0.1'

def run_client(host=DEFAULT_HOST, port=DEFAULT_PORT):
    """Connects to the PyRedis server and provides an interactive prompt."""
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        print(f"Connecting to PyRedis server at {host}:{port}...")
        client_socket.connect((host, port))
        print("Connected! Type 'QUIT' to exit.")

        while True:
            try:
                command_line = input(f"{host}:{port}> ")
                if not command_line:
                    continue

                client_socket.sendall(f"{command_line}\n".encode('utf-8'))

                if command_line.strip().upper() == "QUIT":
                    response_data = client_socket.recv(4096)
                    print(f"Server response: {response_data.decode('utf-8').strip()}")
                    break

                response_data = client_socket.recv(4096)
                if not response_data:
                     print("Connection closed by server unexpectedly.")
                     break

                print(f"Server response: {response_data.decode('utf-8').strip()}")

            except KeyboardInterrupt:
                print("\nDetected Ctrl+C. Sending QUIT to server...")
                client_socket.sendall("QUIT\n".encode('utf-8'))
                try:
                    response_data = client_socket.recv(1024)
                    print(f"Server response: {response_data.decode('utf-8').strip()}")
                except Exception:
                    pass
                break
            except EOFError:
                 print("\nInput stream closed. Sending QUIT.")
                 try:
                     client_socket.sendall("QUIT\n".encode('utf-8'))
                     response_data = client_socket.recv(1024)
                     print(f"Server response: {response_data.decode('utf-8').strip()}")
                 except Exception:
                     pass
                 break

    except ConnectionRefusedError:
        print(f"Error: Connection refused. Is the server running at {host}:{port}?")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing connection.")
        client_socket.close()

if __name__ == "__main__":
    server_host = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_HOST
    server_port = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_PORT
    run_client(server_host, server_port)
