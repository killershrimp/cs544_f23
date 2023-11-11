import argparse
import threading
import pandas as pd
import torch
import grpc
import modelserver_pb2
import modelserver_pb2_grpc

parser = argparse.ArgumentParser(prog="client",description="client server",epilog="")
parser.add_argument("port")
parser.add_argument("coef")
parser.add_argument("threadfiles", nargs="*")
args = parser.parse_args()


port = args.port
addr = f"127.0.0.1:{port}"
coef = args.coef
thread_files = args.threadfiles

channel = grpc.insecure_channel(addr)
stub = modelserver_pb2_grpc.ModelServerStub(channel)

def parse_coef(coef):
    coef = coef.split(",")
    for i in range(len(coef)):
        coef[i] = float(coef[i])
    return torch.tensor(coef).float()

stub.SetCoefs(modelserver_pb2.SetCoefsRequest(coefs=parse_coef(coef)))

def handle_csv(fname):
    data = torch.from_numpy(pd.read_csv(fname, header=None).values).float()
    hits = 0
    for i in data:
        resp = stub.Predict(modelserver_pb2.PredictRequest(X=i))
        if resp.hit:
            hits += 1
    print(hits / len(data)) # hit rate

threads = []
for i in thread_files:
    threads.append(threading.Thread(target=handle_csv, args=(i,)))
    threads[-1].start()

for i in thread_files:
    threads[-1].join()

