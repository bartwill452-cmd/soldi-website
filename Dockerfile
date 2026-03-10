FROM python:3.11-slim

# Install Node.js 20.x and supervisor
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl supervisor && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies (cache layer)
COPY soldi-api/requirements.txt ./soldi-api/
RUN pip install --no-cache-dir -r soldi-api/requirements.txt

# Install Playwright Chromium + system dependencies for browser-based scrapers
# (DraftKings, BetOnline, Caesars, Bet105, BetUS, Bookmaker, Buckeye)
RUN playwright install --with-deps chromium

# Install Node.js dependencies (cache layer)
COPY package.json package-lock.json ./
RUN npm ci --production

# Copy all application code
COPY . .

# Ensure data directory exists for bot persistence (user-tracking.json, etc.)
RUN mkdir -p /app/data

# Set up supervisor config
COPY supervisord.conf /etc/supervisor/conf.d/soldi.conf

# Internal communication: Node.js website (port 3000) → SoldiAPI (port 3001)
# Render routes external traffic to the Node.js website port
ENV PORT=3000
ENV SOLDI_API_URL=http://localhost:3001
EXPOSE 3000

CMD ["supervisord", "-n", "-c", "/etc/supervisor/conf.d/soldi.conf"]
