# set base image (host OS)
FROM python:3.8

# set environment variable
ENV MQTT_SERVER_HOST=
ENV MQTT_SERVER_PORT=
ENV MQTT_USERNAME=
ENV MQTT_PASSWORD=

# set the working directory in the container
WORKDIR /code

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip3 install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY src/ .

# command to run on container start
CMD [ "python", "./example.py" ]
