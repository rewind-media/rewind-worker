FROM debian:11-slim
RUN apt update -y && apt upgrade -y
RUN apt install -y ffmpeg

# TODO verify download at a minimum, ideally find a better way to install latest version of nodejs
RUN apt install -y curl
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && apt-get install -y nodejs

WORKDIR /rewind/worker

COPY . .
RUN rm -rf node_modules dist

RUN npm install
RUN npm run build

CMD ["npm", "run", "start"]