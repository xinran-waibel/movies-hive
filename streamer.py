import time
import socket
from threading import Thread
from argparse import ArgumentParser

file_path = "grouplens_data/ratings/ratings.csv"

def client_talk(client_sock, client_addr):
    print('talking to {0}'.format(client_addr))   
    with open(file_path, 'r') as f:
	f.next()
	for line in f:
		print line
		client_sock.send(line)
		time.sleep(5)
    # clean up
    client_sock.shutdown(1)
    client_sock.close()
    print('connection closed.')

class EchoServer:
  def __init__(self, host, port):
    print('talking on port {0}'.format(port))
    self.host = host
    self.port = port
    self.setup_socket()
    self.accept()
    self.sock.shutdown()
    self.sock.close()

  def setup_socket(self):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.bind((self.host, self.port))
    self.sock.listen(128)

  def accept(self):
    while True:
      (client, address) = self.sock.accept()
      th = Thread(target=client_talk, args=(client, address))
      th.start()

def parse_args():
  parser = ArgumentParser()
  parser.add_argument('--host', type=str, default='localhost',
                      help='specify a host to operate on (default: localhost)')
  parser.add_argument('-p', '--port', type=int, default=3333,
                      help='specify a port to operate on (default: 3333)')
  args = parser.parse_args()
  return (args.host, args.port)


if __name__ == '__main__':
  (host, port) = parse_args()
  EchoServer(host, port)
