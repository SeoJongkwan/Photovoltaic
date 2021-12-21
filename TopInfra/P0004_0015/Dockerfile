FROM python:3.7.3

COPY . ./top15_client

WORKDIR /top15_client

RUN apt-get update && apt-get install -y default-jre

RUN pip install --upgrade pip 

RUN pip install -r client_requirements.txt

ENV TZ Asia/Seoul

EXPOSE 2255

CMD ["python", "top15_client.py"]
