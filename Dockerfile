FROM node:18.9.1
WORKDIR /srv/app
COPY package*.json ./
RUN npm install
COPY . ./
EXPOSE 5280
CMD ["npx", "ts-node", "src/index.ts"]