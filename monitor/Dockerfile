FROM node:10

RUN mkdir /monitor
WORKDIR /monitor
COPY package*.json ./
RUN npm install
COPY *.js /monitor/

ENTRYPOINT ["/monitor/index.js"]
