#!/bin/bash
# Combined startup script — runs both SoldiAPI (Python) and soldi-website (Node.js)
# SoldiAPI runs on port 3001 (internal), website runs on PORT (Render-assigned)

export SOLDI_API_URL=http://localhost:3001

# Start SoldiAPI in background
cd soldi-api && python3 -m uvicorn main:app --host 0.0.0.0 --port 3001 &
SOLDI_PID=$!
echo "SoldiAPI started (PID $SOLDI_PID) on port 3001"

# Give SoldiAPI a moment to start
sleep 2

# Start Node.js website in foreground
cd ..
exec node server.js
