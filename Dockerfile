FROM node:gallium-bullseye-slim AS base
ENV NODE_ENV=production

WORKDIR /app

ADD package.json .
ADD yarn.lock .
RUN yarn --production --frozen-lockfile

ADD index.ts .
ADD utils.ts .

ENTRYPOINT [ "yarn", "start" ]
