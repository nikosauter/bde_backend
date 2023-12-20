FROM alpine:edge

ENV VIRTUAL_ENV=/opt/venv
WORKDIR /opt/venv

RUN apk update && apk upgrade && apk add python3-dev build-base librdkafka-dev py3-pip
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN pip3 install --upgrade pip

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY src src
CMD ["python3", "src/publisher_programm.py"]