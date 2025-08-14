from fastapi import FastAPI
from guppy_consumer.api.endpoints import router

app = FastAPI(
    title="Guppy Consumer",
    description="CSV data processor and MongoDB ingestion service for Guppy Funds",
    version="0.1.0"
)

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)