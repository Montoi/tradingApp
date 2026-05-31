FROM node:20-bookworm-slim

# Install system dependencies (ffmpeg for media, python for streamlink)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install streamlink via pip to get a recent version
RUN pip3 install --no-cache-dir streamlink --break-system-packages

WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install

# Copy application code
COPY . .

# Build TypeScript to dist/
RUN npm run build

# The command will be overridden by docker-compose.yml 
# depending on whether it's the extractor or the api service.
CMD ["npm", "start"]
