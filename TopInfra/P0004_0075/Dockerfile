FROM python:3.7.3

COPY . ./top75_client

WORKDIR /top75_client

RUN apt-get update && apt-get install -y default-jre

RUN pip install --upgrade pip 

RUN pip install -r client_requirements.txt

ENV TZ Asia/Seoul

EXPOSE 2277

CMD ["python", "top75_client.py"]
