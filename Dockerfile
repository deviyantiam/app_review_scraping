FROM python:3.10-slim 

# run update
RUN set -ex && apt-get update

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

RUN printf '#!/usr/bin/env bash  \n\
exec python /app/main.py "$@"\
' >> /app/entrypoint.sh

RUN chmod 700 /app/entrypoint.sh
ENTRYPOINT [ "/app/entrypoint.sh" ]