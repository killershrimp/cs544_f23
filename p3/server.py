from collections import OrderedDict
import torch
import modelserver_pb2
import modelserver_pb2_grpc
import threading
import traceback

class PredictionCache:
    def __init__(self):
        self.lock = threading.Lock()
        self.k_C = 10
        self.cache = []     # elements closer to index=0 are older
        self.coefs = None
    def SetCoefs(self, coefs):
        self.coefs = coefs
        self.cache = []
    def Predict(self, X):
        X = torch.round(X, decimals=4)
        tup_X = tuple(X.flatten().tolist())
        rv = None
        cached = True
        with self.lock:
            rv = self.get(tup_X)
            if rv is None:
                cached = False
                rv = X @ self.coefs
                self.add((tup_X, rv))
        return rv, cached
    def get(self, X):
        el = -1
        for e in range(len(self.cache)):
            if self.cache[e][0] == X:
                el = e
                break
        if el == -1:
            return None
        temp = self.cache[e]
        del self.cache[e]
        self.cache.append(temp)
        return temp[1]
    def add(self, tup):
        if len(self.cache) == self.k_C:
            self.remove_last()
        self.cache.append(tup)
    def remove_last(self):
        if len(self.cache) != 0:
            del self.cache[0]

class ModelServer(modelserver_pb2_grpc.ModelServerServicer):
    def __init__(self):
        super().__init__()
        self.cache = PredictionCache()
    def SetCoefs(self, coefs, context):
        try:
            rv = modelserver_pb2.SetCoefsResponse()
            rv.error = ""

            # c_tensor = []
            # for i in Coefs.coefs:
            #     c.append(i)
            c_tensor = torch.tensor(coefs.coefs)
            self.cache.SetCoefs(c_tensor)
            return rv
        except:
            traceback.print_exc()
            return modelserver_pb2.SetCoefsResponse(error=traceback.format_exc())
    def Predict(self, X, context):
        try:
            # x_tensor = []
            # for i in X.X:
            #     x_tensor.append(i)
            x_tensor = torch.tensor(X.X)

            val = None
            cached = False
            error = ""
            
            val, cached = self.cache.Predict(x_tensor)
            return modelserver_pb2.PredictResponse(y=val,hit=cached, error="")
        except:
            traceback.print_exc()
            return modelserver_pb2.PredictResponse(error=traceback.format_exc())

# server code
import grpc
from concurrent import futures
server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
modelserver_pb2_grpc.add_ModelServerServicer_to_server(ModelServer(), server)
server.add_insecure_port("[::]:5440", )
server.start()
server.wait_for_termination()
