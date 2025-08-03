# backend/main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import logging
from scrapper import scrape_quotes, save_to_file

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class ScrapeRequest(BaseModel):
    topic: str
    format: str = 'json'

@app.post("/scrape")
async def scrape_endpoint(request: ScrapeRequest):
    try:
        quotes = []
        
        def progress_callback(current, total):
            print(f"Progress: {current}/{total}")
            # Ici vous pourriez envoyer des mises à jour via WebSocket
        
        quotes = await scrape_quotes(request.topic, progress_callback)
        
        if request.format in ['json', 'csv']:
            file_data = save_to_file(quotes, request.format)
            return {
                "status": "success",
                "data": quotes,
                "file_data": file_data,
                "format": request.format
            }
        
        return {"status": "success", "data": quotes}
    
    except Exception as e:
        logging.error(f"Error in scraping: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))