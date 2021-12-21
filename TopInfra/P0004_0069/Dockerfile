FROM python:3.7.3

COPY P0004_0069 ./top69_client

WORKDIR /top69_client

RUN apt-get update && apt-get install -y default-jre

RUN pip install --upgrade pip 

RUN pip install -r client_requirements.txt

ENV TZ Asia/Seoul

EXPOSE 2277

CMD ["python", "top69_client.py"]
