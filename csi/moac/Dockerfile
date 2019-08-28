FROM node:10

RUN mkdir /moac
WORKDIR /moac
COPY moac/package*.json ./
RUN npm install
COPY moac /moac/
RUN mkdir -p /mayastor-client/proto
COPY mayastor-client/proto /mayastor-client/proto

ENTRYPOINT ["/moac/index.js"]
