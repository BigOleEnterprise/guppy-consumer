# Guppy Consumer

A FastAPI-based microservice for processing CSV data and ingesting it into MongoDB as part of the Guppy Funds ecosystem.

## Overview

Guppy Consumer is responsible for:
- Accepting CSV files as HTTP payloads
- Performing data transformations on the CSV data
- Inserting processed data into MongoDB

## Features

- **FastAPI**: High-performance async web framework
- **CSV Processing**: Robust CSV parsing and validation
- **MongoDB Integration**: Efficient data insertion with proper error handling
- **Data Transformation**: Configurable data processing pipeline
- **Type Safety**: Full type hints with Pydantic models

## Requirements

- Python 3.11+
- MongoDB instance
- uv package manager

## Installation

```bash
# Install dependencies using uv
uv sync

# Install development dependencies
uv sync --group dev
```

## Development

```bash
# Run the application
uv run uvicorn guppy_consumer.main:app --reload

# Run tests
uv run pytest

# Code formatting
uv run black .
uv run ruff check .

# Type checking
uv run mypy .
```

## API Endpoints

- `POST /upload` - Upload and process CSV files
- `GET /health` - Health check endpoint

## Configuration

Environment variables:
- `MONGODB_URL` - MongoDB connection string
- `DATABASE_NAME` - MongoDB database name
- `COLLECTION_NAME` - MongoDB collection name

## License

MIT