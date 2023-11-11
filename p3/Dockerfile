FROM ubuntu:22.04

RUN apt-get update && apt-get install -y unzip python3 python3-pip
RUN pip3 install pandas==2.1.2
RUN pip3 install torch==2.0.1 --index-url https://download.pytorch.org/whl/cpu
RUN pip3 install grpcio==1.58.0 grpcio-tools==1.58.0

COPY --chmod==555 /client.py .
COPY --chmod=555 /modelserver.proto .
COPY --chmod=555 /server.py .
COPY --chmod==555 /workload/ .

RUN ["python3", "-m", "grpc_tools.protoc", "-I=.", "--python_out=.", "--grpc_python_out=.", "modelserver.proto"]
CMD ["python3", "server.py"]
