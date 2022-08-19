FROM node:gallium-bullseye-slim AS base
ENV NODE_ENV=production

WORKDIR /app

ADD . .

RUN yarn --production --frozen-lockfile

ENTRYPOINT [ "yarn", "start" ]
