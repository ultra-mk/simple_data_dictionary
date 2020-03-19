FROM python:3.8-buster as build

ARG SSH_PRIVATE_KEY

WORKDIR /builtin

RUN pip install --upgrade pip wheel setuptools
ADD requirements.txt requirements-dev.txt ./
RUN mkdir /root/.ssh/ \
    && echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa \
    && chmod 600 /root/.ssh/id_rsa \
    && eval "$(ssh-agent -s)" \
    && ssh-add /root/.ssh/id_rsa \
    && touch /root/.ssh/known_hosts \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts \
    && pip install -r requirements-dev.txt \
    && pip install -r requirements.txt \
    && rm -rf /root/.ssh
ADD mypy.ini ./mypy.ini

FROM python:3.8-slim-buster as deploy
WORKDIR /builtin
COPY --from=build /usr/local/bin/ /usr/local/bin/
COPY --from=build /usr/local/lib/python3.8/site-packages/ /usr/local/lib/python3.8/site-packages/
ADD . .

RUN pip install .