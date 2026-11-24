FROM node:22-bookworm-slim

WORKDIR /app

# Build tools for native modules without prebuilt binaries (serialport, bacstack deps)
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 make g++ \
 && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-optional

COPY src ./src
# Default configuration: copied into config/ at startup when missing there, so a volume
# mounted on config/ (Kubernetes emptyDir, empty Docker volume) keeps transformers, DDS
# example and storage defaults. Writable directories belong to the runtime user.
COPY config ./config.defaults
RUN mkdir -p config certificates logs \
 && cp -R config.defaults/. config/ \
 && chown -R node:node config certificates logs

ENV NODE_ENV=production \
    PORT=3000

EXPOSE 3000
USER node
CMD ["node", "src/server.js"]
