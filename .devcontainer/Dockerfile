FROM mcr.microsoft.com/devcontainers/python:3.6-bullseye

ADD https://dl.yarnpkg.com/debian/pubkey.gpg /etc/apt/trusted.gpg.d/yarn.asc

RUN chmod +r /etc/apt/trusted.gpg.d/*.asc && \
    echo "deb http://dl.yarnpkg.com/debian/ stable main" > /etc/apt/sources.list.d/yarn.list