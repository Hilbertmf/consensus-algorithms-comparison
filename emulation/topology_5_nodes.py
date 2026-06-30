def start_test():
    h1 = net.get('h1') # node-1 on port 50051
    h2 = net.get('h2') # node-2 on port 50051

    h1.cmd('./test_messaging node-1 50051 node-2 10.0.0.2:50051 &')
    h2.cmd('./test_messaging node-2 50051 node-1 10.0.0.1:50051 &')