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

def generate_move_sequence_id(moves):
    """Generate a unique ID for a move sequence."""
    return "_".join(moves) if moves else "root"

def initialize_move_stats():
    """Initialize statistics for a move sequence."""
    return {
        "white_win": 0,
        "draw": 0,
        "black_win": 0,
        "next_moves": set()  # Using set to avoid duplicates
    }

def process_csv_to_mongo(csv_file_path, database_name="chess_db", collection_name="openings", batch_size=1000, max_depth=6):
    """
    Process CSV file and create individual documents for each move sequence.
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
        
        # Dictionary to store all move sequences and their statistics
        move_sequences = defaultdict(initialize_move_stats)
        
        print(f"Processing CSV file: {csv_file_path}")
        print(f"Max depth: {max_depth}")
        
        # Process CSV file
        processed_games = 0
        batch_count = 0
        
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                result = normalize_result(row.get('result', ''))
                moves_string = get_moves_from_row(row)
                moves = parse_moves(moves_string)
                
                if not moves or not result:
                    continue
                
                # Add the first move to root's next moves
                if moves:
                    move_sequences["root"]["next_moves"].add(moves[0])
                    move_sequences["root"][result] += 1  # Count all games in root

                # Process all subsequences of moves up to max_depth
                for depth in range(1, min(len(moves) + 1, max_depth + 1)):
                    subsequence = moves[:depth]
                    sequence_id = generate_move_sequence_id(subsequence)
                    
                    # Update statistics for this subsequence
                    move_sequences[sequence_id][result] += 1
                    
                    # Add next move if exists
                    if depth < len(moves):
                        next_move = moves[depth]
                        move_sequences[sequence_id]["next_moves"].add(next_move)
                
                processed_games += 1
                
                # Print progress
                if processed_games % batch_size == 0:
                    batch_count += 1
                    print(f"Processed {processed_games} games (batch {batch_count})")
        
        print(f"Total games processed: {processed_games}")
        print(f"Total move sequences found: {len(move_sequences)}")
        
        # Convert to documents and insert into MongoDB
        print("Preparing documents for MongoDB...")
        
        documents = []
        document_count = 0
        
        for sequence_id, stats in move_sequences.items():
            # Calculate percentages
            total_games = stats["white_win"] + stats["draw"] + stats["black_win"]
            
            if total_games == 0:
                continue
            
            # Parse move sequence from ID
            move_sequence = sequence_id.split("_") if sequence_id != "root" else []

            next_moves_list = []

            for next_move in stats["next_moves"]:
                # Create the sequence ID for this next move
                next_sequence = move_sequence + [next_move]
                next_sequence_id = generate_move_sequence_id(next_sequence)
                
                # Look up the statistics for this specific next move sequence
                if next_sequence_id in move_sequences:
                    next_move_stats = move_sequences[next_sequence_id]
                    next_total_games = next_move_stats["white_win"] + next_move_stats["draw"] + next_move_stats["black_win"]
                                        
                    next_moves_list.append({
                        "name": next_move,
                        "white_win": next_move_stats["white_win"],
                        "draw": next_move_stats["draw"],
                        "black_win": next_move_stats["black_win"],
                        "total_games": next_total_games,
                    })
            
            # Sort next moves by total games (most popular first)
            next_moves_list.sort(key=lambda x: x["total_games"], reverse=True)
            
            document = {
                "_id": sequence_id,
                "move_sequence": move_sequence,
                "depth": len(move_sequence),
                "white_win": stats["white_win"],
                "draw": stats["draw"],
                "black_win": stats["black_win"],
                "total_games": total_games,
                "next_moves": next_moves_list,  # Use the enriched list instead of just names
            }
            
            documents.append(document)
            document_count += 1
            
            # Insert in batches to avoid memory issues
            if len(documents) >= 1000:
                collection.insert_many(documents)
                print(f"Inserted batch of {len(documents)} documents (total: {document_count})")
                documents = []
        
        # Insert remaining documents
        if documents:
            collection.insert_many(documents)
            print(f"Inserted final batch of {len(documents)} documents")
        
        print("Inserted summary document")
        
        # Create indexes for efficient querying
        print("Creating indexes...")
        try:
            collection.create_index("move_sequence")
            collection.create_index("depth")
            collection.create_index("total_games")
            collection.create_index([("depth", 1), ("total_games", -1)])  # Compound index
            print("Successfully created indexes")
        except Exception as e:
            print(f"Error creating indexes: {e}")
        
        # Print final statistics
        total_docs = collection.count_documents({})
        
        print("\nDatabase Statistics:")
        print(f"- Total documents: {total_docs}")
        print(f"- Move sequence documents: {document_count}")
        print(f"- Total games processed: {processed_games}")
        print(f"- Max depth: {max_depth}")
        print(f"- Database: {database_name}")
        print(f"- Collection: {collection_name}")
        
        # Show some example queries
        print("\nExample move sequences:")
        
        # Most popular first moves
        first_moves = list(collection.find(
            {"depth": 1}, 
            {"move_sequence": 1, "total_games": 1}
        ).sort("total_games", -1).limit(5))
        
        print("\nTop 5 first moves by popularity:")
        for doc in first_moves:
            move = doc["move_sequence"][0]
            games = doc["total_games"]
        
        # Example of accessing specific opening
        e4_e5 = collection.find_one({"_id": "e4_e5"})
        if e4_e5:
            print(f"\nExample: e4 e5 opening")
            print(f"  Total games: {e4_e5['total_games']:,}")
        
        return True
        
    except FileNotFoundError:
        print(f"Error: Could not find {csv_file_path}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        client.close()

def main():
    csv_file = 'reduce_csv/reduced_chess_games.csv'
    
    print("Chess Opening Tree CSV to MongoDB Loader")
    print("=" * 50)
    
    # Parameters
    max_depth = 8  # Can handle deeper sequences now
    batch_size = 1000  # Progress reporting frequency
    
    success = process_csv_to_mongo(
        csv_file,
        database_name="goapi",
        max_depth=max_depth, 
        batch_size=batch_size
    )
    
    if success:
        print("\n✅ Data successfully loaded to MongoDB!")
    else:
        print("\n❌ Failed to load data to MongoDB")

if __name__ == "__main__":
    main()