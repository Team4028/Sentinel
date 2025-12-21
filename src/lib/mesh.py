import time
import sys
from pubsub import pub
from meshtastic.serial_interface import SerialInterface
from meshtastic.serial_interface import serial

# loosely sourced from https://github.com/brad28b/meshtastic-cli-receive-text/blob/main/read_messages_serial.py

serial_port = '/dev/ttyUSB0'

def on_receive(packet, f):
    try:
        if packet['decoded']['portnum'] == 'TEXT_MESSAGE_APP':
            message = packet['decoded']['payload'].decode('utf-8')
            f(message)
    except KeyError:
        pass
    except UnicodeDecodeError:
        pass

def send_mesh_test(message: str):
    """ Fake recieving `message` from a mesh radio text """
    packet = {
        "to": 0,
        "from": 1,
        "decoded": {
            "portnum": "TEXT_MESSAGE_APP",
            "payload": message.encode("utf-8")
        }
    }
    pub.sendMessage("meshtastic.receive", packet=packet, interface=None) # 'recieve' the payload

def main(f):
    def on_receive_wrapper(packet, interface):
        on_receive(packet, f)

    pub.subscribe(on_receive_wrapper, "meshtastic.receive") # bind the listener to the recieve channel
    try: 
        local = SerialInterface(serial_port)
        print(f"SerialInterface setup for listening on port {serial_port}")
    except Exception as e:
        print(f"Error opening port {serial_port}: {e}")
        print("This channel will remain open for testing purposes.")
        
    try: # listen forever
        while True:
            sys.stdout.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        local.close()