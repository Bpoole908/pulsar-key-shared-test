import prometheus_client as pc
import random
import time

import numpy as np
# Create a metric to track time spent and requests made.
s = pc.Summary('request_processing_seconds', 'Time spent processing request')
h = pc.Histogram('request_latency_seconds', 'Description of histogram')
i = pc.Info('my_build_version', 'Description of info')

DIC = {}
# Summary
@s.time()
def summary():
    for i in range(10):
        s.observe(i) 

@h.time()
def histogram():
    for i in range(10):
        h.observe(i) 

def info():
    i.info(DIC)
    DIC[str(np.random.randint(0, 9999))] = str(time.time())

if __name__ == '__main__':
    # Start up the server to expose the metrics.
    pc.start_http_server(8000)
    # Generate some requests.
    while True:
        summary()
        histogram()
        info()
        time.sleep(1)