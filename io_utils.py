import ijson
import json
import os

def read_json_stream(path):
    """
    Reads JSON files with OData format.
    Streams users from the 'value' array without loading the entire file into memory.
    """
    with open(path, "r", encoding="utf-8") as f:
        # Use ijson to stream from the 'value' array in OData format
        for user in ijson.items(f, "value.item"):
            yield user

def write_json_stream(path, data_iterable):
    """
    Writes transformed users to a JSON file as a simple array.
    Streams data without loading everything into memory.
    """
    with open(path, "w", encoding="utf-8") as f:
        f.write("[\n")
        first = True
        for item in data_iterable:
            if not first:
                f.write(",\n")
            f.write(json.dumps(item, ensure_ascii=False, indent=2))
            first = False
        f.write("\n]")

# Write batches of 100 users to separate files
def write_batches(output_dir, data_iterable, batch_size=100):
    batch = []
    file_idx = 0
    for user in data_iterable:
        batch.append(user)
        if len(batch) == batch_size:
            batch_path = os.path.join(output_dir, f"users_{file_idx:03d}.json")
            write_json_stream(batch_path, batch)
            print(f"Wrote {len(batch)} users to {batch_path}")
            batch = []
            file_idx += 1
    if batch:
        batch_path = os.path.join(output_dir, f"users_{file_idx:03d}.json")
        write_json_stream(batch_path, batch)
        print(f"Wrote {len(batch)} users to {batch_path}")
