import time
from dataclasses import dataclass, asdict
from threading import Thread
import zmq
import logging
import pickle

# from ..consts import RUNNING, INIT, STOPPED
# from ..dtypes import Message
from queue import Queue, Empty

from zolo.consts import USER_MSG_GATEWAY

log = logging.getLogger(__name__)

context = zmq.Context()
socket = context.socket(zmq.PAIR)


INIT = "INIT"
RUNNING = "RUNNING"
STOPPED = "STOPPED"


@dataclass(frozen=True)
class Message:
    cmd: str
    payload: dict


class UserMessageGateway:
    
    def __init__(self, end_point: str):
        self._end_point = end_point
        self._state = INIT
        self._thread: Thread = None
        self._z_poll = zmq.Poller()
        self._z_poll.register(socket, zmq.POLLIN)
        socket.bind(f"tcp://{self._end_point}:5555")
        super().__init__()
    
    @property
    def is_running(self):
        return self._state == RUNNING
    
    def _poll(self, q: Queue):
        self._state = RUNNING
        while self.is_running:
            try:
                msg = self._poll_once()
            except TimeoutError:
                continue
            q.put(msg)
        self._state = STOPPED
    
    def _poll_once(self):
        res = self._z_poll.poll(timeout=1)
        if not res:
            raise TimeoutError
        msg: bytes = socket.recv()
        if msg:
            msg = pickle.loads(msg)
            print(f"{msg}")
            return msg
        
        raise TimeoutError
    
    def reboot(self, q: Queue):
        self.stop()
        self.start(q)
    
    def stop(self):
        if self.is_running:
            self._state = STOPPED
            self._thread.join(5)
            self._z_poll.unregister(socket)
            if self._state != STOPPED:
                log.error("Try to stop failed!")
    
    def start(self, q: Queue):
        if not self.is_running:
            self._thread = Thread(target=self._poll, args=(q,))
            self._thread.start()


def main():
    q = Queue()
    gw = UserMessageGateway("*")
    gw.start(q)
    client = context.socket(zmq.PAIR)
    client.connect(USER_MSG_GATEWAY)
    while True:
        try:
            msg = q.get(timeout=3)
        except Empty:
            msg = pickle.dumps(
                Message(
                    "START",
                    dict(timeout=5)
                )
            )
            client.send(msg)
        except KeyboardInterrupt:
            gw.stop()
            break
        else:
            print(msg)


if __name__ == '__main__':
    main()
