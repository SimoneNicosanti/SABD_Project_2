FROM python

# RUN apt-get update

# # Java Config
# RUN apt-get install openjdk-17-jdk -y
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Python Config
RUN apt-get update
RUN apt-get -y install python3-pip

# Python Dependencies
RUN pip install kafka-python
RUN pip install jproperties