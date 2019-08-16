FROM node:10
RUN ["mkdir", "/frontend"]
WORKDIR /frontend
COPY package.json package-lock.json ./
ENV NODE_PATH /node_modules
RUN ["npm", "ci"]
COPY ./ ./
RUN ["npm", "run", "build"]
