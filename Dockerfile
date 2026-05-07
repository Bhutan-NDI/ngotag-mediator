FROM node:18-bullseye AS base

RUN corepack enable

# Set cache dir so it can be shared between different docker stages
RUN yarn config set cache-folder /tmp/yarn-cache

FROM base AS setup

# AFJ specifc setup
WORKDIR /www

# Copy root package files
COPY package.json /www/package.json
COPY yarn.lock /www/yarn.lock

# Copy patches folder
COPY patches /www/patches

# Run yarn install
RUN yarn install

COPY tsconfig.build.json /www/tsconfig.build.json
COPY . /www

RUN yarn build

FROM base AS final

WORKDIR /www

COPY --from=setup /www/build /www/build
COPY --from=setup /tmp/yarn-cache /tmp/yarn-cache

# Copy root package files and mediator app package
COPY package.json /www/package.json
COPY yarn.lock /www/yarn.lock

# Copy patches folder
COPY patches /www/patches

WORKDIR /www

# Run yarn install
RUN yarn install --production

# Clean cache to reduce image size
RUN yarn cache clean

ENTRYPOINT [ "yarn", "start" ]
