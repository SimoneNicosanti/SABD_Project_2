FROM flink:latest

# install python3 and pip3
RUN apt-get update -y 
RUN apt install python3 -y
RUN apt-get update -y
RUN apt-get install python3-pip -y

# install PyFlink
RUN pip3 install apache-flink