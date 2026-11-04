FROM node:22-bookworm-slim

WORKDIR /app

# Build tools for native modules without prebuilt binaries (serialport, bacstack deps)
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 make g++ \
 && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-optional

COPY src ./src
COPY config ./config

ENV NODE_ENV=production \
    PORT=3000

EXPOSE 3000
USER node
CMD ["node", "src/server.js"]
