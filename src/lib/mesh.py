import time
import sys
from pubsub import pub
from meshtastic.serial_interface import SerialInterface
import logging
import os
# loosely sourced from https://github.com/brad28b/meshtastic-cli-receive-text/blob/main/read_messages_serial.py

logger = logging.getLogger(__name__)
if os.name == 'posix':
    serial_port = '/dev/ttyUSB0'
elif os.name == 'nt':
    serial_port = 'COM3' # there are no other ports I promise
else:
    logger.warning('What in the jython is this operating system? probably linux but the bootloader is deleted')
    serial_port = '/dev/ttyUSB0'
local = None

def on_receive(packet, f):
    try:
        if packet['decoded']['portnum'] == 'TEXT_MESSAGE_APP':
            message = packet['decoded']['payload'].decode('utf-8')
            f(message)
            if local == None:
                logger.info("'Recived' " + message)
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

def send_message(m):
    global local
    if local:
        local.sendText(m)
    else:
        send_mesh_test(m)

def send_command(cmd, pw_sha):
    send_message(f"@app.cmd --pwd {pw_sha} {cmd}")


def main(f):
    global local # ahh yes
    def on_receive_wrapper(packet, interface):
        on_receive(packet, f)

    pub.subscribe(on_receive_wrapper, "meshtastic.receive") # bind the listener to the recieve channel
    try: 
        local = SerialInterface(serial_port)
        logger.info(f"SerialInterface setup for listening on port {serial_port}")
    except Exception as e:
        local = None
        logger.error(f"Error opening port {serial_port}: {e}\nThis channel will remain open for testing purposes.")
    try: # listen forever
        while True:
            sys.stdout.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        if local != None:
            local.close()