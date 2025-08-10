import math
import time

import simple_tsdb


q = simple_tsdb.PushQueue('localhost', 4000)

while True:
    t_ns = time.time_ns()
    t_ms = (t_ns // 1000000) % 10000;
    v = math.sin((t_ms / 10000) * 2 * math.pi)

    p = {
        'time_ns' : t_ns,
        'value'   : v,
    }
    print(p)
    q.append(p, 'test_db/sine_points/test_series')

    time.sleep(0.1)
