FROM python:latest
RUN apt-get -y update
RUN apt-get install -y python3-pip build-essential
COPY . .
RUN pip3 install -r requirements.txt
ENTRYPOINT ["python3"]
CMD ["app.py"]
