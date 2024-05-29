FROM ubuntu:23.10

ENV DEBIAN_FRONTEND = noninteractive

 
RUN apt-get update && \
    apt-get install -y python3-pip python3-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /appenv


ENV PATH="/appenv/bin:$PATH"

RUN pip install --upgrade pip

RUN pip install grpcio==1.60.1 grpcio-tools==1.60.1

WORKDIR /app

COPY . /app


RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. mathdb.proto

EXPOSE 5440

CMD ["python", "server.py"]
