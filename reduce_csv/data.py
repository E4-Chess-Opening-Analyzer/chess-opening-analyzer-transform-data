# import psycopg2
import csv
import time
import re
import json  # Added for JSON serialization

TOTAL_LINES = 6250000
REGEX = re.compile(r'[!?]+')

# conn = psycopg2.connect(
#     dbname="goapi",
#     user="ugoapi",
#     password="pgoapi",
#     host="localhost",
#     port=5432
# )

# cur = conn.cursor()

start_time = time.time()

with open('chess_games.csv', 'r', newline='') as f:
    reader = csv.reader(f)
    with open('reduced_chess_games.csv', 'w', newline='') as reduced_f:
        writer = csv.writer(reduced_f)
        writer.writerow(['result', 'moves'])
        i = 0
        for row in reader:
            result = 1 if row[3] == '1-0' else -1 if row[3] == '0-1' else 0 if row[3] == '1/2-1/2' else None
            if result is not None:
                move_str = row[14]

                move_split = move_str.split()
                move_split = [m for m in move_split if not m.endswith(".") and not m.replace(".", "").isdigit()]
                move_split = [m for m in move_split if not m in ["{", "}"] and not m.startswith("[") and not m.endswith("]")]
                move_split.pop()
                move_split = move_split[:10]
                move_split = [REGEX.sub('', m) for m in move_split]

                moves_pg = json.dumps(move_split)
                writer.writerow([result, moves_pg])

            i += 1
            if i % 10000 == 0:
                # Comment the following line when using complete dataset
                # break 
                elapsed = time.time() - start_time
                rate = i / elapsed
                remaining = (TOTAL_LINES - i) / rate if rate > 0 else 0
                print(f"{i}/{TOTAL_LINES} | "
                      f"({rate:.0f} lines/sec) | ETA: {remaining/60:.1f} min")

# with open('reduced_chess_games.csv', 'r', newline='') as f:
#     cur.copy_expert("COPY games (result, moves) FROM STDIN WITH CSV HEADER", f)

# conn.commit()
# cur.close()
# conn.close()
