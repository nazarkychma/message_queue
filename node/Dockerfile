FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip3 install flask
RUN pip3 install requests

CMD ["python3", "main.py"]