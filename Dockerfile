##########################
## Build env
##########################
FROM python:3.7-alpine3.10 AS BUILD

RUN apk add gcc postgresql-dev musl-dev libressl-dev libffi-dev make
RUN pip install --upgrade pip

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

##########################
## Final image
##########################
FROM python:3.7-alpine3.10

RUN apk add --no-cache --update libressl postgresql-libs

RUN addgroup beacon && \
    adduser -D -G beacon beacon && \
    mkdir /beacon

COPY beacon_api /beacon/beacon_api
#COPY logger.yaml /beacon/logger.yaml

COPY --from=BUILD usr/local/lib/python3.7/ usr/local/lib/python3.7/

RUN chown -R beacon:beacon /beacon
WORKDIR /beacon
USER beacon
CMD ["python", "-m", "beacon_api"]