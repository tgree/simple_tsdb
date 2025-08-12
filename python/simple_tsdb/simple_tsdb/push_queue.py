# Copyright (c) 2020 by Phase Advanced Sensor Systems, Inc.
# All rights reserved.
import threading
import time

from .client import Client


class PushQueue:
    '''
    Class to asynchronously push data points to a Simple TSDB instance.  Pushing
    points can take a nondeterministic length of time and by trying to push
    them synchronously you can introduce lots of jitter into your measurement
    loop.  This asynchronous queue allows the work to be performed in a separate
    thread so as not to disturb the measurement times.  It also transparently
    deals with write-throttling from the tsdbserver process, and handles
    buffering and reconnecting if the tsdbserver becomes unreachable for some
    reason.
    '''
    def __init__(self, host, port, username=None, password=None,
                 push_cb=None, throttle_secs=0):
        self.push_cb = push_cb

        if username is None or password is None:
            self.tsdb = Client(host, port)
        else:
            self.tsdb = Client(host, port, credentials=(username, password))

        self.queue_cond    = threading.Condition()
        self.queue         = {}
        self.cookie_queue  = {}
        self.schemas       = {}
        self.thread        = None
        self.running       = False
        self.throttle_secs = throttle_secs
        self.start()

    def start(self):
        assert not self.thread
        self.running = True
        self.thread  = threading.Thread(target=self._push_loop, daemon=True)
        self.thread.start()

    def append(self, p, path, cookie=None):
        '''
        Append a single point to the push queue.
        '''
        with self.queue_cond:
            if path not in self.queue:
                self.queue[path] = [p]
                self.cookie_queue[path] = [cookie]
            else:
                self.queue[path].append(p)
                self.cookie_queue[path].append(cookie)
            self.queue_cond.notify()

    def append_list(self, ps, path, cookies=None):
        '''
        Append a list of points to the push queue.
        '''
        if cookies is None:
            cookies = [None] * len(ps)
        with self.queue_cond:
            if path not in self.queue:
                self.queue[path] = ps[:]
                self.cookie_queue[path] = cookies[:]
            else:
                self.queue[path] += ps
                self.cookie_queue[path] += cookies
            self.queue_cond.notify()

    def flush(self):
        '''
        Flush the current queue elements.
        '''
        while self.queue:
            pass
        with self.queue_cond:
            self.running = False
            self.queue_cond.notify()
        self.thread.join()
        self.thread = None
        self.start()

    def _push_loop(self):
        while self.queue or self.running:
            time.sleep(self.throttle_secs)

            with self.queue_cond:
                while not self.queue and self.running:
                    self.queue_cond.wait()

                queue             = self.queue
                cookies           = self.cookie_queue
                self.queue        = {}
                self.cookie_queue = {}

            if queue:
                for path, points in queue.items():
                    database, measurement, series = path.split('/')
                    schema = self.schemas.get((database, measurement))
                    while True:
                        try:
                            if schema is None:
                                schema = self.tsdb.get_schema(database,
                                                              measurement)
                                self.schemas[(database, measurement)] = schema
                            self.tsdb.write_points(database, measurement,
                                                   series, schema, points)
                            break
                        except Exception as e:
                            print('TSDB push exception: %s' % e)
                            print('Retrying in 30 seconds...')
                            time.sleep(30)

                    if self.push_cb:
                        for p, c in zip(points, cookies[path]):
                            self.push_cb(p, c)
