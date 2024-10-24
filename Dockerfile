FROM alpine:3.9

RUN apk add --update --no-cache py3-pip && \
    pip3 install boto3

ADD restic_mon.py /restic_mon.py

ENV PYTHONUNBUFFERED=1

CMD /restic_mon.py
