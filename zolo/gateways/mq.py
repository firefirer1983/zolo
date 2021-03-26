import json
import time
from dataclasses import dataclass, asdict
from json import JSONDecodeError
from threading import Thread
import zmq
import logging

from ..consts import RUNNING, INIT, STOPPED
from ..dtypes import Message
from queue import Queue, Empty

from zolo.consts import USER_MSG_GATEWAY

log = logging.getLogger(__name__)
#
# INIT = "INIT"
# RUNNING = "RUNNING"
# STOPPED = "STOPPED"
#
#
# @dataclass(frozen=True)
# class Message:
#     cmd: str
#     payload: dict


class ZmqGateway:
    
    def __init__(self, host: str):
        self._state = INIT
        self._thread: Thread = None
        self._ctx = zmq.Context()
        self._sock = self._ctx.socket(zmq.PAIR)
        self._poller = zmq.Poller()
        self._poller.register(self._sock, zmq.POLLIN)
        self._sock.bind(f"{host}")
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
        res = self._poller.poll(timeout=1)
        if not res:
            raise TimeoutError
        msg: bytes = self._sock.recv()
        if msg:
            try:
                msg = json.loads(msg, encoding="utf8")
                return Message(cmd=msg["cmd"], payload=msg["payload"])
            except (KeyError, JSONDecodeError):
                log.warning(f"invalid msg: {msg}")
                return
        
        raise TimeoutError
    
    def reboot(self, q: Queue):
        self.stop()
        self.start(q)
    
    def stop(self):
        if self.is_running:
            self._state = STOPPED
            self._thread.join(5)
            self._poller.unregister(self._sock)
            if self._state != STOPPED:
                log.error("Try to stop failed!")
    
    def start(self, q: Queue):
        if not self.is_running:
            self._thread = Thread(target=self._poll, args=(q,))
            self._thread.start()


def main():
    context = zmq.Context()
    q = Queue()
    gw = ZmqGateway("tcp://*:5555")
    gw.start(q)
    client = context.socket(zmq.PAIR)
    client.connect(USER_MSG_GATEWAY)
    while True:
        try:
            msg = q.get(timeout=3)
        except Empty:
            res = json.dumps(
                asdict(Message(
                    "START",
                    dict(timeout=5)
                ))
            ).encode("utf8")
            client.send(res)
        except KeyboardInterrupt:
            gw.stop()
            break
        else:
            print(msg)


if __name__ == '__main__':
    main()
