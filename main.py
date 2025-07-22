from transformer import UserTransformer
from io_utils import read_json_stream, write_batches
from concurrent.futures import ThreadPoolExecutor
import itertools
import os
import glob

def process_users(input_directory, output_dir, chunk_size=100):
    """
    Processes user data from multiple JSON files.
    Writes every 100 transformed users to a separate JSON file in output_dir.
    """
    transformer = UserTransformer()
    file_errors = []

    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    def transformed_users_generator():
        # Find all JSON files in the input directory
        json_files = glob.glob(os.path.join(input_directory, "*.json"))
        json_files.sort()
        print(f"Processing {len(json_files)} JSON files from {input_directory}")

        for file_path in json_files:
            try:
                # Stream users from each file
                print(f"Processing file: {os.path.basename(file_path)}")
                users = read_json_stream(file_path)
            except Exception as e:
                # Record and skip files that fail to read
                file_errors.append((file_path, f"Read error: {e}"))
                continue

            file_failed = False
            file_results = []

            while True:
                # Read users in chunks for parallel transformation
                chunk = list(itertools.islice(users, chunk_size))
                if not chunk:
                    break

                try:
                    # Transform users in parallel using ThreadPoolExecutor
                    chunk_results = list(ThreadPoolExecutor().map(transformer.transform, chunk))
                    file_results.extend(chunk_results)
                except Exception as e:
                    # Record and skip files that fail to transform
                    file_errors.append((file_path, f"Chunk transform error: {e}"))
                    file_failed = True
                    break

            if not file_failed:
                # Yield transformed users from successfully processed files
                yield from file_results

    try:
        # Write transformed users in batches to output directory
        write_batches(output_dir, transformed_users_generator(), chunk_size)
    except Exception as e:
        raise e

    # Print any errors encountered during processing
    if file_errors:
        print("\nErrors occurred while processing files:")
        for file_path, error in file_errors:
            print(f"{file_path}: {error}")

if __name__ == "__main__":
    try:
        # Run the user processing pipeline
        process_users("usersapi", "transformed_users")
    except Exception as e:
        print(f"Fatal error: {e}")