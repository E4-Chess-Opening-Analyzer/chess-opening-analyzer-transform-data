import csv
import os
from collections import defaultdict
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError

def connect_to_mongo():
    """Connect to MongoDB using environment variables."""
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://admin:password123@mongo:27017/')
    
    try:
        client = MongoClient(mongo_uri)
        # Test the connection
        client.admin.command('ping')
        print(f"Successfully connected to MongoDB at {mongo_uri}")
        return client
    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
        return None

def parse_moves(moves_string):
    """
    Parse a string of chess moves and return a list of individual moves.
    Example: "[""e4"", ""e5"", ""Nf3"", ""Nc6""]" -> ["e4", "e5", "Nf3", "Nc6"]
    """
    if not moves_string or moves_string.strip() == '':
        return []
    
    moves = []
    moves_string = moves_string.strip()
    if moves_string.startswith('[') and moves_string.endswith(']'):
        # Remove brackets and split by comma
        moves = [move.strip().strip('"').strip("'") for move in moves_string[1:-1].split(',')]
    else:
        # Fallback: split by spaces
        moves = moves_string.split()
    
    return moves

def get_moves_from_row(row):
    """Extract moves string from CSV row."""
    return row.get('moves', '') or row.get('opening_moves', '') or row.get('first_moves', '')

def normalize_result(result):
    """Normalize game result to standard format."""
    if isinstance(result, (int, float)):
        result = str(int(result))
    else:
        result = str(result).strip()
    
    if result == '1':
        return 'white_win'
    elif result == '-1':
        return 'black_win'
    elif result == '0':
        return 'draw'
    return None

def initialize_move_node():
    """Create a new move node with default statistics."""
    return {
        "white_win": 0,
        "draw": 0,
        "black_win": 0,
        "next": {}
    }

def update_tree_statistics(tree, moves, result_type, max_depth=5):
    """Update the tree with a sequence of moves and result."""
    if not moves or max_depth <= 0:
        return
    
    current_level = tree
    for i, move in enumerate(moves[:max_depth]):
        if move not in current_level:
            current_level[move] = initialize_move_node()
        
        # Update statistics for this move
        if result_type:
            current_level[move][result_type] += 1
        
        # Move to next level
        if i < len(moves) - 1 and i < max_depth - 1:
            if "next" not in current_level[move]:
                current_level[move]["next"] = {}
            current_level = current_level[move]["next"]

def calculate_percentages(tree):
    """Calculate win percentages for all moves in the tree."""
    for move, data in tree.items():
        total_games = data["white_win"] + data["draw"] + data["black_win"]
        
        if total_games > 0:
            data["white_win_rate"] = round(data["white_win"] / total_games * 100, 2)
            data["draw_rate"] = round(data["draw"] / total_games * 100, 2)
            data["black_win_rate"] = round(data["black_win"] / total_games * 100, 2)
        else:
            data["white_win_rate"] = data["draw_rate"] = data["black_win_rate"] = 0.0
        
        data["total_games"] = total_games
        
        # Recursively calculate for next moves
        if "next" in data and data["next"]:
            calculate_percentages(data["next"])

def estimate_document_size(data):
    """Rough estimate of BSON document size."""
    import json
    return len(json.dumps(data, separators=(',', ':')))

def process_csv_to_mongo(csv_file_path, database_name="chess_db", collection_name="openings", batch_size=1000, max_depth=4):
    """
    Process CSV file and load directly into MongoDB with memory optimization.
    Split into multiple documents to avoid 16MB BSON limit.
    """
    # Connect to MongoDB
    client = connect_to_mongo()
    if not client:
        return False
    
    try:
        # Get database and collection
        db = client[database_name]
        collection = db[collection_name]
        
        # Clear existing data
        print("Clearing existing data...")
        collection.drop()
        
        # Initialize tree structure
        opening_tree = {}
        
        print(f"Processing CSV file: {csv_file_path}")
        print(f"Max depth: {max_depth}")
        
        # Process CSV in batches
        processed_games = 0
        batch_count = 0
        
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                result = normalize_result(row.get('result', ''))
                moves_string = get_moves_from_row(row)
                moves = parse_moves(moves_string)
                
                if not moves:
                    continue
                
                # Update tree with this game
                update_tree_statistics(opening_tree, moves, result, max_depth)
                
                processed_games += 1
                
                # Print progress every batch_size games
                if processed_games % batch_size == 0:
                    batch_count += 1
                    print(f"Processed {processed_games} games (batch {batch_count})")
        
        print(f"Total games processed: {processed_games}")
        print("Calculating win percentages...")
        
        # Calculate percentages for all moves
        calculate_percentages(opening_tree)
        
        print(f"Found {len(opening_tree)} different first moves")
        
        # Insert documents separately for each first move
        print("Inserting documents into MongoDB...")
        
        # First, insert a summary document
        summary_doc = {
            "_id": "summary",
            "total_first_moves": len(opening_tree),
            "total_games_processed": processed_games,
            "max_depth": max_depth,
            "created_at": "2024-01-01T00:00:00Z",
            "first_moves": list(opening_tree.keys())
        }
        
        collection.insert_one(summary_doc)
        print("Inserted summary document")
        
        # Then insert each first move as a separate document
        inserted_count = 0
        for first_move, move_data in opening_tree.items():
            try:
                document = {
                    "_id": f"first_move_{first_move}",
                    "first_move": first_move,
                    "data": move_data,
                    "created_at": "2024-01-01T00:00:00Z"
                }
                
                # Check estimated size
                estimated_size = estimate_document_size(document)
                if estimated_size > 15000000:  # 15MB threshold
                    print(f"Warning: Document for {first_move} is large ({estimated_size/1000000:.1f}MB), reducing depth...")
                    # Reduce the tree depth for this move
                    reduced_data = reduce_tree_depth(move_data, 2)
                    document["data"] = reduced_data
                
                collection.insert_one(document)
                inserted_count += 1
                
                if inserted_count % 5 == 0:
                    print(f"Inserted {inserted_count}/{len(opening_tree)} first move documents")
                
            except Exception as e:
                print(f"Error inserting document for {first_move}: {e}")
                continue
        
        # Create indexes
        print("Creating indexes...")
        try:
            collection.create_index("first_move")
            collection.create_index("data.total_games")
            print("Successfully created indexes")
        except Exception as e:
            print(f"Error creating indexes: {e}")
        
        # Print statistics
        total_docs = collection.count_documents({})
        
        print("\nDatabase Statistics:")
        print(f"- Total documents: {total_docs}")
        print(f"- First move documents: {inserted_count}")
        print(f"- Total games processed: {processed_games}")
        print(f"- Max depth: {max_depth}")
        print(f"- Database: {database_name}")
        print(f"- Collection: {collection_name}")
        
        # Show top 5 first moves
        if opening_tree:
            first_moves_stats = []
            for move, stats in opening_tree.items():
                total_games = stats.get("total_games", 0)
                first_moves_stats.append((move, total_games))
            
            first_moves_stats.sort(key=lambda x: x[1], reverse=True)
            
            print("\nTop 5 first moves by number of games:")
            for move, count in first_moves_stats[:5]:
                win_rate = opening_tree[move].get("white_win_rate", 0)
                print(f"  {move}: {count} games (White win rate: {win_rate}%)")
        
        return True
        
    except FileNotFoundError:
        print(f"Error: Could not find {csv_file_path}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        client.close()

def reduce_tree_depth(tree, max_depth, current_depth=0):
    """Reduce the depth of a tree to fit within size limits."""
    if current_depth >= max_depth:
        return {}
    
    reduced_tree = {}
    for move, data in tree.items():
        reduced_data = {
            "white_win": data.get("white_win", 0),
            "draw": data.get("draw", 0),
            "black_win": data.get("black_win", 0),
            "white_win_rate": data.get("white_win_rate", 0),
            "draw_rate": data.get("draw_rate", 0),
            "black_win_rate": data.get("black_win_rate", 0),
            "total_games": data.get("total_games", 0)
        }
        
        if "next" in data and current_depth < max_depth - 1:
            reduced_data["next"] = reduce_tree_depth(data["next"], max_depth, current_depth + 1)
        else:
            reduced_data["next"] = {}
        
        reduced_tree[move] = reduced_data
    
    return reduced_tree

def main():
    csv_file = 'reduce_csv/reduced_chess_games.csv'
    
    print("Chess Opening Tree CSV to MongoDB Loader")
    print("=" * 50)
    
    # Reduced parameters to avoid BSON size limits
    max_depth = 4  # Reduced depth
    batch_size = 1000  # Progress reporting frequency
    
    success = process_csv_to_mongo(
        csv_file, 
        max_depth=max_depth, 
        batch_size=batch_size
    )
    
    if success:
        print("\n✅ Data successfully loaded to MongoDB!")
    else:
        print("\n❌ Failed to load data to MongoDB")

if __name__ == "__main__":
    main()