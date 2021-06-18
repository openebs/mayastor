FROM node:16.3.0-alpine3.12

RUN apk --no-cache add curl netcat-openbsd

WORKDIR /moac
COPY package*.json tsconfig.json src crds moac mbus.js README.md ./
COPY src ./src/
COPY crds ./crds/
COPY scripts ./scripts/
RUN npm install
RUN npm run compile
RUN ln -s /moac/moac /bin/moac
ENTRYPOINT ["/bin/moac"]
EXPOSE 3000/tcp