# Filename: Dockerfile

FROM alpine:3.20.1

WORKDIR /tmp
RUN apk update && apk add \
    tzdata \
    git \
    python3 \
    py3-pip \
    musl-dev \
    gcc \
    python3-dev \
    py3-virtualenv

RUN cp /usr/share/zoneinfo/US/Pacific /etc/localtime
RUN echo "US/Pacific" > /etc/timezone
RUN apk del tzdata



WORKDIR /usr/local/share/api
COPY app.py .
COPY .env .
COPY requirements.txt .

RUN mkdir -p src
COPY src src


RUN virtualenv .venv
RUN . .venv/bin/activate && pip install -r requirements.txt

CMD [".venv/bin/python", "app.py"]
