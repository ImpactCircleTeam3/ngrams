FROM python:3.8

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY ./main.py /main.py
COPY ./requirements.txt /requirements.txt

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

CMD ["python", "main.py"]
