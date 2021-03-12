import time
from logging import getLogger
from threading import Thread
from queue import Queue
from datetime import datetime

from zolo.consts import INIT, RUNNING, STOPPED, UNIX_EPOCH
from zolo.dtypes import Timer


log = getLogger(__name__)


class TimerGen:
    
    def __init__(self):
        self._ts = UNIX_EPOCH
        self._state = INIT
        self._thread: Thread = None
    
    @property
    def is_running(self):
        return self._state == RUNNING
    
    def _poll(self, q: Queue):
        self._state = RUNNING
        while self.is_running:
            msg = self._poll_once()
            if not msg:
                time.sleep(.1)
                continue
            q.put(msg)

        self._state = STOPPED
    
    def _poll_once(self):
        
        if (datetime.utcnow() - self._ts).seconds > 1:
            self._ts = datetime.utcnow()
            return Timer(timestamp=self._ts)

    def reboot(self, q: Queue):
        self.stop()
        self.start(q)
    
    def stop(self):
        if self.is_running:
            self._state = STOPPED
            self._thread.join(5)
            if self._state != STOPPED:
                log.error("Try to stop failed!")
    
    def start(self, q: Queue):
        if not self.is_running:
            self._thread = Thread(target=self._poll, args=(q,))
            self._thread.start()
