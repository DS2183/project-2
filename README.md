# Data Analyst Agent

This Python FastAPI project exposes a POST `/api/` endpoint that receives a data analysis question, automatically sources, prepares, analyzes, and visualizes data as described in your prompt (see `app.py`). Answers are returned in the requested structured format, including base64-encoded plots.

## Run locally

pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8080

text

Send requests with:
curl -X POST "http://localhost:8080/api/" -F "@question.txt"

text
undefined
