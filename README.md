# Chess Opening Analyzer - Transform Data

This project transforms chess game data from CSV format into a MongoDB database, creating an opening tree structure for analysis.

## Overview

The system processes chess game data to build a hierarchical opening tree that tracks move statistics, win rates, and game outcomes. The data is stored in MongoDB for efficient querying and analysis.

## Project Structure

```
.
├── csv_to_mongo.py          # Main script to load CSV data into MongoDB
├── compose.yaml             # Docker Compose configuration
├── Dockerfile               # Python application container
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables
└── reduce_csv/              # CSV processing utilities
    ├── data.py              # Script to reduce/clean CSV data
    ├── chess_games.csv      # Original chess games dataset
    ├── reduced_chess_games.csv # Processed CSV with essential data
    └── README.md            # CSV processing documentation
```

## Features

- **Memory Efficient Processing**: Processes large CSV files without loading everything into memory
- **Opening Tree Generation**: Creates hierarchical chess opening trees with statistics
- **Multiple Document Storage**: Splits data across multiple MongoDB documents to avoid 16MB BSON limits
- **Win Rate Calculations**: Automatically calculates win percentages for each move
- **Configurable Depth**: Adjustable tree depth to manage memory and document size
- **Progress Reporting**: Real-time processing progress updates

## Data Structure

### Input CSV Format
The system expects a CSV file with the following columns:
- `result`: Game outcome (1 = White win, 0 = Draw, -1 = Black win)
- `moves`: JSON array of chess moves (first 10 moves of each game)

### MongoDB Document Structure
Each first move is stored as a separate document:

```json
{
  "_id": "first_move_e4",
  "first_move": "e4",
  "data": {
    "white_win": 1250000,
    "draw": 800000,
    "black_win": 950000,
    "white_win_rate": 41.67,
    "draw_rate": 26.67,
    "black_win_rate": 31.67,
    "total_games": 3000000,
    "next": {
      "e5": {
        "white_win": 520000,
        "draw": 350000,
        "black_win": 430000,
        "white_win_rate": 40.0,
        "draw_rate": 26.92,
        "black_win_rate": 33.08,
        "total_games": 1300000,
        "next": { ... }
      }
    }
  }
}
```

A summary document tracks overall statistics:
```json
{
  "_id": "summary",
  "total_first_moves": 20,
  "total_games_processed": 6254841,
  "max_depth": 4,
  "first_moves": ["e4", "d4", "Nf3", ...]
}
```

## Setup and Usage

### Prerequisites
- Docker and Docker Compose
- CSV file with chess game data

### Environment Variables
Configure these in your `.env` file:
```env
MONGO_URI=mongodb://admin:password123@mongo:27017/
MONGO_INITDB_ROOT_USERNAME=admin
MONGO_INITDB_ROOT_PASSWORD=password123
```

### Running the Application

1. **Start the services**:
   ```bash
   docker compose up -d
   ```

2. **Process your CSV data**:
   Place your `reduced_chess_games.csv` in the `reduce_csv/` folder and run:
   ```bash
   docker compose up python
   ```

3. **Access MongoDB**:
   - MongoDB: `localhost:27017`
   - Mongo Express (Web UI): `http://localhost:8081`

### Configuration Options

You can modify the processing parameters in [`csv_to_mongo.py`](csv_to_mongo.py):

- `max_depth`: Maximum depth of the opening tree (default: 4)
- `batch_size`: Progress reporting frequency (default: 1000)
- Database and collection names

## Data Processing Pipeline

1. **CSV Reading**: Reads chess game data from CSV file
2. **Move Parsing**: Extracts and parses chess moves from JSON format
3. **Tree Building**: Builds hierarchical opening tree with move statistics
4. **Percentage Calculation**: Computes win rates for each move
5. **Document Splitting**: Splits large trees into manageable MongoDB documents
6. **Database Storage**: Stores documents in MongoDB with proper indexing

## Services

### Python Application
- **Image**: Python 3.9 slim
- **Memory**: 2GB limit, 1GB reservation
- **Function**: Processes CSV and loads data into MongoDB

### MongoDB
- **Image**: MongoDB latest
- **Port**: 27017
- **Health Check**: Automatic connection verification
- **Storage**: Persistent volume for data

### Mongo Express
- **Image**: Mongo Express latest
- **Port**: 8081
- **Function**: Web-based MongoDB administration interface

## Performance Considerations

- **Memory Usage**: Configurable memory limits prevent out-of-memory errors
- **Document Size**: Automatic splitting prevents BSON size