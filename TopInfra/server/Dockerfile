FROM python:3.7.3

COPY . ./top_server

WORKDIR /top_server

RUN apt-get update && apt-get install -y default-jre

RUN pip install --upgrade pip 

RUN pip install -r server_requirements.txt

ENV JAVA_OPTS="-Xmx4096M"

ENV TZ Asia/Seoul

EXPOSE 2255

CMD ["python", "top_server.py"]
