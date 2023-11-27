FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip3 install flask requests flask_jwt_extended cffi cryptography pycparser

CMD ["python3", "main.py"]