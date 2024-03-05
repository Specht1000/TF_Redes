import binascii
import queue
import socket
import threading
import time 
import random
from dataclasses import dataclass
from enum import Enum

TOKEN_MSG = '1000'.encode('utf-8') # token message
DEFAULT_PORT = 1234 # default port
DEFAULT_CONFIG_FILE = 'config.txt' # default config file
DEFAULT_SOCKET_BUFFER = 1024 # default socket buffer
DEFAULT_TOKEN_TIMEOUT = 50 # seconds to wait for token timeout if machine is token origin
DEFAULT_TOKEN_MIN_TIME = 12 # minimum time for a token to be received if machine is token origin, else it is discarded
DEFAULT_ERROR_PROBABILITY = 0.1 # probability to introduce crc error in message
DEFAULT_MAX_QUEUE_SIZE = 10 # max size for message queue

#TODO: organize timeout method, too messy


# Class to represent a machine
class Node:
    # Constructor for Node class.
    def __init__(
        self, nickname, port, wait_time, has_token,
        next_host, next_port, socket_buffer, is_token_origin
    ):
        self.nickname = nickname
        self.port = port
        self.wait_time = wait_time # time between operations
        self.has_token = has_token # if machine has token
        self.next_host = next_host 
        self.next_port = next_port 
        self.socket_buffer = socket_buffer # socket buffer size
        self.is_token_origin = is_token_origin # if machine is token origin
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # create socket
        self.socket.bind(('0.0.0.0', self.port)) # bind socket to port
        self.q = queue.Queue(DEFAULT_MAX_QUEUE_SIZE) # queue to store messages up to specified size
        self.token_count = 0 # token count for timeout logic
        self.time_sent = 0 # time sent for min time logic
        self.time_received = 0 # time received for min time logic
        if self.is_token_origin: # if machine is the token origin, send token
            self.send_token()
  

    # Function to receive messages from server.
    def receive_messages(self):
        while True:
            try: 
                # Receive message from server.
                response, address = self.socket.recvfrom(self.socket_buffer)
                response_aux = response.decode('utf-8')
                print(f'\nMessage {response_aux} received from {address[0]}:{address[1]}')
                
                # Process message through new thread
                thread = threading.Thread(target=self.process_messages, args=(response,))
                thread.start()
                    


            except Exception as e:
                print(f"An error occurred while receiving messages: {e}")
        
    # Function to process messages from a client
    def process_messages(self, msg):   
        aux = msg.decode('utf-8')
        time.sleep(self.wait_time) # wait token time to process message
        if ( msg== TOKEN_MSG): # if message is token

                    self.has_token = True # set has token to true
                    if self.is_token_origin: # check if machine is token origin and apply min time logic
                        self.time_received = time.time()
                        if (self.time_received - self.time_sent) < DEFAULT_TOKEN_MIN_TIME:
                            print ('\nToken received too soon -> discarded')
                            return
             
                    # check if message queue is empty, if not, send message
                    if not self.q.empty():
                        message = self.q.get()
                        self.send_message(message)
                        self.q.put(message)
                        return
            
                    # if queue is empty, send token
                    self.send_token()
                    return

        # if message is not token, adapt message to object and process it
        msg_response = create_msg_object_response(msg.decode('utf-8'))
        if msg_response.dest == self.nickname: # if message is for this machine
            msg_response.reply = self.define_reply(msg_response)
            self.send_message(msg_response)
        elif msg_response.src == self.nickname: # if message is from this machine
            self.process_message_source(msg_response)
        else:
            self.send_message(msg_response)
                    

    def queue_msg(self, dest_user, msg: str) -> bool: # function to queue message
        try:
            msg = Msg(2000, self.nickname, dest_user, Reply.MACHINE_DOES_NOT_EXIST.value, msg, binascii.crc32(msg.encode()))
            self.q.put(msg)
            print (f'\nMessage {msg.decode()} added to queue')
        except Exception as e:
            print(f"An error occurred while adding message to queue: {e}")
            return False
        return True
    
    def send_token(self): # function to send token
        if self.is_token_origin:
            self.time_sent = time.time()
        self.has_token = False
        self.token_count += 1
        print (f'\nSending token to {self.next_host}:{self.next_port}')
        self.socket.sendto(TOKEN_MSG, (self.next_host, self.next_port))

    def send_message(self, msg: 'Msg'): # function to send message
       
        if random.random() < DEFAULT_ERROR_PROBABILITY:
            msg.crc = 0
        print (f'\nSending message ({msg.decode()}) to {self.next_host}:{self.next_port}')
        self.socket.sendto(msg.decode(), (self.next_host, self.next_port))

    def define_reply(self, msg: 'Msg') -> 'Reply': # function to define reply type
        if msg.dest == self.nickname:
            crc = binascii.crc32(msg.data.encode()) 
            print (f'crc {crc} msg.crc {msg.crc}')
            if crc != int(msg.crc):
                
                return Reply.NAK.value
            else:
                return Reply.ACK.value
        else:
          return msg.reply
        
    def process_message_source(self, msg: 'Msg'): # function to process received message on source machine
        if msg.dest == 'TODOS':
            self.q.get()
            print (f'\nBroadcast {msg.data} looped back')
            self.send_token()
            return
        else:
            if msg.reply == Reply.ACK.value:
                self.q.get()
                print (f'\nACK {msg.data} on source')
                self.send_token()
                return
            if msg.reply == Reply.NAK.value:
                msg.reply = Reply.MACHINE_DOES_NOT_EXIST.value
                msg.crc = binascii.crc32(msg.data.encode())
                print(f'\nNAK Error {msg.data}')
                self.send_message(msg)
                return
            else:
                self.q.get()
                print (f'\nMachine {msg.dest} not found')
                self.send_token()
                return



@dataclass # dataclass to represent message object
class Msg():
    code: int
    src: str 
    dest: str 
    reply: 'Reply'
    crc: int
    data: str

    # Constructor for Msg class
    def __init__(self, code, src, dest, reply, data, crc):
        self.code = code
        self.src = src 
        self.dest = dest 
        self.reply = reply
        self.data = data
        self.crc = crc

    # Function to decode message into bytes
    def decode(self) -> bytes:
        return f'{self.code};{self.src}:{self.dest}:{self.reply}:{self.crc}:{self.data}'.encode()
    


# Enum to represent reply types
class Reply(Enum):
    MACHINE_DOES_NOT_EXIST = 'maquinanaoexiste'
    NAK = 'NAK'
    ACK = 'ACK'

# Function to configure machine and initialize it.
def config_node(file: str) -> Node :
    file_exists = check_file_exists(file)

    if file_exists:
        with open(file, 'r') as fp:
            lines = iter(fp.readlines())

            lines = [line.strip() for line in lines]
            host_port = lines[0].split(':')
            next_host, next_port = host_port[0], int(host_port[1])
            nickname = lines[1]
            wait_time = int(lines[2])
            is_origin = lines[3].strip().lower() == "true"
            has_token = is_origin
            #print (f'Configuring node {nickname} with token time {wait_time} and token {has_token} and origin: {is_origin}')
            return Node(
                nickname, DEFAULT_PORT, wait_time, has_token, 
                next_host, next_port, DEFAULT_SOCKET_BUFFER,is_origin
            )
    else:
        print("File does not exist")
        

# Function to check if file exists.
def check_file_exists(file_name):
    try: 
        file = open(file_name, "r")
        file.close()
        return True
    except:
        return False

# Function to create message object from string message
def create_msg_object_response(msg: str) -> 'Msg':
    
    code, rest = msg.split(';')
    src, dest, reply, crc, data = rest.split(':')
    return Msg(code, src, dest, reply, data, crc)

# Function to send token after timeout
def token_timeout(node: Node):
    if node.is_token_origin:
        last_token = node.token_count + 1
        total_seconds = DEFAULT_TOKEN_TIMEOUT
        timeout = False
        while True:
            if node.token_count != last_token:
                last_token = node.token_count
                total_seconds = DEFAULT_TOKEN_TIMEOUT
                while total_seconds > 0:
                    time.sleep(1)
                    total_seconds -= 1
                    if node.token_count != last_token:
                        last_token = node.token_count
                        timeout = False
                        break
                if total_seconds == 0:
                    timeout = True
                if last_token == node.token_count and timeout:
                    print ('\nToken timeout, sending token again')
                    timeout = False
                    node.send_token()

# Function to process user input based on message type     
def send_msg_based_type(node: Node, type: int) -> bool:
    if type == 1:
        # Get destination and message from user.
        msg_sent = input('\nEnter destination Nickname and message (dest msg ...): ')
        msg_sent = msg_sent.split(' ')
        if len(msg_sent) < 2:
            print('Invalid input')
            return False
        dest_user = msg_sent[0]
        msg = ' '.join(msg_sent[1:])
        return node.queue_msg(dest_user, msg)
    elif type == 2:
        msg_sent = input('Enter message: ')
        return node.queue_msg('TODOS', msg_sent)
    elif type == 3:
        node.send_token()
        return True
    elif type == 4:
        print('Quitting...')
        exit(0)



# Main function.
def main():
    # Configure machine.
    node = config_node(DEFAULT_CONFIG_FILE)


    # Start receive threads.
    receive_thread = threading.Thread(target=node.receive_messages)
    receive_thread.daemon = True
    receive_thread.start()
    
    # If token origin is machine, start timeout thread.
    if node.is_token_origin:
        timeout_thread = threading.Thread(target=token_timeout, args=(node,))
        timeout_thread.daemon = True
        timeout_thread.start()

    print("Welcome!")
   
   # Loop to get user input.
    while True:
         try:
            # Get message type from user.
            msg_type = input('\n|----Enter message type (1 Unicast, 2 Broadcast, 3 Insert Token, 4 Quit)----|')
            try :
                msg_type = int(msg_type)
            except:
                print('Invalid input')
                continue
            send_msg_based_type(node, msg_type) # process user input
            
         except Exception as e:
            print(f"An error occurred: {e}")
            break


if __name__ == "__main__":
    main()
