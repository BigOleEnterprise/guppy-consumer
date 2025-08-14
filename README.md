# Guppy Consumer

Enterprise-grade FastAPI microservice for CSV data processing and MongoDB ingestion as part of the Guppy Funds ecosystem.

## Overview

Guppy Consumer is an enterprise-grade data ingestion service that provides:

- **Intelligent CSV Processing**: Automatic detection and parsing of Amex and Wells Fargo CSV formats
- **Duplicate Prevention**: Hash-based duplicate detection with batch processing
- **Raw Data Storage**: Preserves original transaction data in bank-specific MongoDB collections
- **Production Monitoring**: Comprehensive health checks, system metrics, and admin endpoints
- **API Documentation**: Auto-generated Swagger/OpenAPI documentation

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CSV Upload    │ -> │  Bank Detection  │ -> │  Data Parsing   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         v                       v                       v
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Duplicate Check │ -> │  Batch Insert    │ -> │ MongoDB Storage │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

**Supported Banks**: Amex, Wells Fargo  
**Collections**: `amex_raw`, `wells_raw` in `guppy_funds` database

## Features

### Core Processing
- **Bank Detection**: Automatic CSV format detection using header analysis
- **Data Validation**: Pydantic-based type validation with error handling
- **Duplicate Prevention**: SHA256 hash-based duplicate detection
- **Bulk Operations**: High-performance batch processing with partial failure handling
- **Error Recovery**: Graceful degradation and comprehensive error reporting

### Enterprise Features  
- **Health Monitoring**: Database connectivity and collection accessibility checks
- **System Metrics**: CPU, memory, disk usage monitoring via `/admin/system/info`
- **Performance Monitoring**: Processing time tracking and throughput metrics
- **Database Administration**: Index management and optimization tools
- **Structured Logging**: Enterprise-grade logging with timestamps and levels

### API Documentation
- **Swagger UI**: Interactive API documentation at `/docs`
- **ReDoc**: Alternative API documentation at `/redoc`
- **OpenAPI**: Machine-readable API specification at `/openapi.json`

## Requirements

- **Python**: 3.11+
- **MongoDB**: Running instance with connection string
- **Package Manager**: uv (recommended) or pip
- **Memory**: 512MB+ recommended for large CSV processing
- **Disk**: Space for MongoDB collections and logs

## Installation

### Using uv (Recommended)
```bash
# Clone repository
git clone <repository-url>
cd guppy-consumer

# Install dependencies
uv pip install -e ".[dev]"

# Install dependencies simpler
uv sync

# Copy environment configuration
cp .env.example .env
# Edit .env with your MongoDB connection string
```

### Using uv
```bash
pip install -r requirements.txt  # Generated from pyproject.toml
```

## Configuration

### Environment Variables (.env)
```bash
# MongoDB Configuration
MONGODB_URL=mongodb+srv://user:password@cluster.mongodb.net/
DATABASE_NAME=guppy_funds
AMEX_COLLECTION=amex_raw
WELLS_COLLECTION=wells_raw

# Application Configuration  
ENVIRONMENT=development
LOG_LEVEL=INFO
```

### MongoDB Collections
- **`amex_raw`**: Raw Amex transaction data with 13 fields + metadata
- **`wells_raw`**: Raw Wells Fargo transaction data with 5 fields + metadata

Both collections include:
- `raw_hash`: SHA256 hash for duplicate detection (unique index)
- `created_at`: Record insertion timestamp (index)
- `bank_type`: Bank identifier for collection routing

## Usage

### Development
```bash
# Start development server with hot reload
uv run uvicorn guppy_consumer.main:app --reload --host 0.0.0.0 --port 8000

# View API documentation
open http://localhost:8000/docs
```

### Production
```bash
# Start production server
uv run uvicorn guppy_consumer.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## API Endpoints

### Core Operations
- **`POST /api/v1/upload/`** - CSV upload and processing pipeline
- **`GET /api/health`** - Application and database health check  
- **`GET /api/stats`** - Processing statistics and collection metrics

### Data Query  
- **`GET /api/query/amex/sample`** - Sample Amex transactions for inspection
- **`GET /api/query/wells/sample`** - Sample Wells Fargo transactions
- **`GET /api/query/collections/stats`** - Collection document counts

### System Administration
- **`GET /api/admin/system/info`** - System metrics (CPU, memory, disk, uptime)
- **`GET /api/admin/database/indexes`** - Database index information  
- **`POST /api/admin/database/reindex`** - Rebuild database indexes

## CSV Processing Pipeline

1. **Upload Validation**: File type, size limits (50MB), encoding detection
2. **Bank Detection**: Automatic format identification (Amex/Wells Fargo)
3. **Data Parsing**: CSV parsing with Pydantic validation
4. **Hash Generation**: Transaction hashing for duplicate detection
5. **Duplicate Filtering**: Batch duplicate check against MongoDB
6. **Bulk Insertion**: High-performance batch insert with error handling
7. **Response Generation**: Detailed processing metrics and results

## Monitoring & Operations

### Health Checks
```bash
# Application health
curl http://localhost:8000/api/health

# System metrics  
curl http://localhost:8000/api/admin/system/info

# Database indexes
curl http://localhost:8000/api/admin/database/indexes
```

### Logs
```bash
# View application logs
tail -f logs/guppy-consumer.log

# Filter by level
grep "ERROR" logs/guppy-consumer.log
```

## Performance

### Benchmarks
- **Processing Speed**: ~1000 transactions/second
- **Memory Usage**: ~100MB base + ~1MB per 1000 transactions
- **Duplicate Detection**: O(1) hash lookups with batch queries
- **Database Operations**: Bulk inserts with connection pooling

### Optimization
- Use batch uploads (1000+ transactions) for best performance
- Monitor system metrics via `/admin/system/info`
- Rebuild indexes periodically via `/admin/database/reindex`

## Error Handling

### HTTP Status Codes
- **200**: Success with processing details
- **400**: Invalid file format or encoding
- **413**: File too large (>50MB)
- **422**: Unsupported CSV format or parsing failure
- **500**: Internal server error

### Logging Levels
- **INFO**: Normal operations, processing summaries
- **WARNING**: Non-critical issues, duplicate detection
- **ERROR**: Processing failures, database connection issues
- **DEBUG**: Detailed processing steps (development only)

## Development

### Project Structure
```
guppy_consumer/
├── api/                    # FastAPI endpoints
│   ├── endpoints.py       # Core upload/health endpoints
│   ├── query_endpoints.py # Data query endpoints  
│   └── admin_endpoints.py # System administration
├── models/raw/            # Pydantic models
│   ├── amex_raw.py       # Amex transaction schema
│   └── wells_raw.py      # Wells Fargo transaction schema
├── parsers/               # CSV processing
│   ├── detector.py       # Bank format detection
│   ├── amex.py          # Amex parser
│   └── wells_fargo.py   # Wells Fargo parser
├── services/             # Business logic
│   ├── mongodb_service.py # Database operations
│   └── raw/              # Raw data processing
└── config/               # Configuration management
```

### Adding New Bank Support
1. Create parser in `parsers/new_bank.py`
2. Add Pydantic model in `models/raw/new_bank_raw.py`  
3. Update `BankDetector` in `parsers/detector.py`
4. Add collection configuration in `config/settings.py`

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
- Check logs for detailed error messages
- Use `/api/admin/system/info` for system diagnostics
- Review API documentation at `/docs`