FROM python:3.7.3

COPY . ./top23_client

WORKDIR /top23_client

RUN apt-get update && apt-get install -y default-jre

RUN pip install --upgrade pip 

RUN pip install -r client_requirements.txt

ENV TZ Asia/Seoul

EXPOSE 2255

CMD ["python", "top23_client.py"]
