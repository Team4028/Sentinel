import time
import sys
from pubsub import pub
from meshtastic.serial_interface import SerialInterface
from meshtastic.serial_interface import serial

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

def main(f):
    def on_receive_wrapper(packet, interface):
        on_receive(packet, f)

    pub.subscribe(on_receive_wrapper, "meshtastic.receive")
    try: 
        local = SerialInterface(serial_port)
    except serial.SerialException as e:
        print(f"Error opening port {serial_port}: {e}")
        return
        
    print(f"SerialInterface setup for listening on port {serial_port}")
    try:
        while True:
            sys.stdout.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        local.close()