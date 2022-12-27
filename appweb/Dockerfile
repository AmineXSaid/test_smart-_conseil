FROM ubuntu 

RUN apt-get update -y  && apt-get install -y python3-pip
RUN apt-get install python3.5

RUN pip install flask 
RUN pip install detoxify
RUN pip install pandas
RUN pip install re
COPY ap.py /opt/

RUN mkdir /opt/templates

COPY templates/simple.html /opt/templates/simple.html

RUN mkdir /opt/Tweets sentiment analysis Harassement


COPY /home/amine/Desktop/Tweets sentiment analysis Harassement/harassements.csv /opt/Tweets sentiment analysis Harassement/

ENTRYPOINT FLASK_APP=/opt/ap.py flask run --host=0.0.0.0

