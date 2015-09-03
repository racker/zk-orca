FROM node:0.12

RUN mkdir -p /src
WORKDIR /src

ONBUILD RUN npm install
