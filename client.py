import grpc
import sys
import threading
import csv
from concurrent.futures import ThreadPoolExecutor
import mathdb_pb2
import mathdb_pb2_grpc

# Global variables
hits = 0
miss = 0
lock = threading.Lock()

def process_csv(file_path, channel):
    global hits, miss
    stub = mathdb_pb2_grpc.MathDbStub(channel)

    with open(file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            command, keys = row[0], row[1:]
            try:
                # Checks if command is in stub
                if command in ['add', 'sub', 'mult', 'div']:
                    # Binary operations
                    if (command == 'add'):
                        response = stub.Add(mathdb_pb2.BinaryOpRequest(key_a=keys[0], key_b=keys[1]))
                    elif (command == 'sub'):
                        response = stub.Sub(mathdb_pb2.BinaryOpRequest(key_a=keys[0], key_b=keys[1]))
                    elif (command == 'mult'):
                        response = stub.Mult(mathdb_pb2.BinaryOpRequest(key_a=keys[0], key_b=keys[1]))
                    else:
                        response = stub.Div(mathdb_pb2.BinaryOpRequest(key_a=keys[0], key_b=keys[1]))
                    with lock:
                        if response.cache_hit:
                            hits += 1
                        else:
                            miss += 1
                elif command == 'set':
                    # Set operation
                    response = stub.Set(mathdb_pb2.SetRequest(key=keys[0], value=float(keys[1])))
                elif command == 'get':
                    # Get operation
                    response = stub.Get(mathdb_pb2.GetRequest(key=keys[0]))
                else:
                    print(f"Unsupported command: {command}")
            except grpc.RpcError as e:
                print(f"RPC failed: {e}")

def main(port, csv_files):
    channel = grpc.insecure_channel(f'localhost:{port}')
    threads = []

    # Create and start thread for each csv file
    for file_path in csv_files:
        thread = threading.Thread(target=process_csv, args=(file_path, channel))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    total_operations = hits + miss
    hit_rate = hits / total_operations if total_operations > 0 else 0
    print(f"{hit_rate}")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("python3 client.py <PORT> <CSV_FILE_PATH.csv> ...")
        sys.exit(1)   
    port = sys.argv[1]
    csv_files = sys.argv[2:]
    main(port, csv_files)

                    
