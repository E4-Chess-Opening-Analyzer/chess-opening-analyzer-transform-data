# Chess Opening Analyzer - Transform Data

This project transforms chess game data from CSV format into a MongoDB database, creating an opening tree structure for analysis.

## Overview

The system processes chess game data to build a hierarchical opening tree that tracks move statistics, win rates, and game outcomes. The data is stored in MongoDB for efficient querying and analysis.

## Project Structure

```
.
├── csv_to_mongo.py          # Main script to load CSV data into MongoDB
├── compose.yaml             # Docker Compose configuration*
├── compose.test.yaml        # Docker Compose configuration**
├── Dockerfile               # Python application container
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables
└── reduce_csv/              # CSV processing utilities
    ├── data.py              # Script to reduce/clean CSV data
    ├── chess_games.csv      # Original chess games dataset
    ├── reduced_chess_games.csv # Processed CSV with essential data
    └── README.md            # CSV processing documentation
```
* This ```compose.yml``` requires to have the API's database running in the background
** Used to test the script without needing to have the API's database running in the backgroud

## Features

- **Memory Efficient Processing**: Processes large CSV files without loading everything into memory
- **Opening Tree Generation**: Creates hierarchical chess opening trees with statistics
- **Multiple Document Storage**: Splits data across multiple MongoDB documents to avoid 16MB BSON limits
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
    _id: "d4_d5",
    move_sequence: [
        "d4",
        "d5"
    ],
    depth: 2,
    white_win: 336018,
    draw: 25728,
    black_win: 277376,
    total_games: 639122,
    next_moves: [
        {
            name: "c4",
            white_win: 159871,
            draw: 11684,
            black_win: 118761,
            total_games: 290316
        },
        {
            name: "Nf3",
            white_win: 58748,
            draw: 4715,
            black_win: 46527,
            total_games: 109990
        },
        ...
    ]
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
MONGO_URI=mongodb://root:rootpass@mongo:27017/
MONGO_INITDB_ROOT_USERNAME=root
MONGO_INITDB_ROOT_PASSWORD=rootpass
```

### Running the Application

You can run the loader in two modes depending on whether you want a local MongoDB for testing or to attach the loader to an existing stack/network.

Option A — Local (recommended for development/testing)

This starts a local MongoDB (`database`) and `mongo-express` alongside the `python` loader. It uses `compose.test.yaml` which includes a `database` service and healthchecks.

1. Ensure your `.env` contains the credentials and `MONGO_URI` (the example `.env` in the repo is suitable for local runs).

2. Start the local stack:

```bash
docker compose -f compose.test.yaml up
```

3. Access services locally:

- MongoDB: `localhost:27017`
- Mongo Express (Web UI): `http://localhost:8081`

Option B — External network / attach to existing stack

Use this when you have a running the API from the api repository and want the loader to join that network. `compose.yaml` attaches the `python` service to the external Docker network named `go-api-network`.

1. Make sure your `.env` `MONGO_URI` points to the correct hostname that is reachable on `go-api-network` (for example `mongodb://root:rootpass@database:27017/?authSource=admin` if the Mongo container on that network is named `database`).

2. Start only the `python` service attached to the external network:

```bash
docker compose -f compose.yaml up
```

### Configuration Options

You can modify the processing parameters in [`csv_to_mongo.py`](csv_to_mongo.py):

- `max_depth`: Maximum depth of the opening tree (default: 4)
- `batch_size`: Progress reporting frequency (default: 1000)
- Database and collection names

