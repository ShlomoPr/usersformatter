from transformer import UserTransformer, BaseTransformer
from io_utils import aread_json_stream, write_batches
import asyncio
import os
import glob
import json
import aiofiles

# Registry for OData context to transformer mapping
ODATA_TRANSFORMER_MAP = {
    "users": UserTransformer,
    # Add more mappings here as needed
}

def get_transformer_class_from_odata(context: str):
    """
    Selects the transformer class based on the OData context string.
    """
    if not context:
        return UserTransformer
    for key, cls in ODATA_TRANSFORMER_MAP.items():
        if key in context:
            return cls
    return UserTransformer

async def get_odata_context(file_path):
    """
    Reads the @odata.context field from the JSON file header asynchronously.
    """
    async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
        # Read a small chunk to parse the header
        content = await f.read(2048)
        try:
            data = json.loads(content)
            return data.get("@odata.context", "")
        except Exception:
            return ""

async def process_users(input_directory, output_dir, chunk_size=100, max_concurrent_files=2):
    """
    Processes user data from multiple JSON files.
    Writes every 100 transformed users to a separate JSON file in output_dir.
    Limits the number of concurrently processed files with max_concurrent_files.
    Automatically selects the transformer based on the OData context.
    """
    file_errors = []

    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    async def process_file(file_path, semaphore, results):
        # Process a single file, respecting the concurrency limit.
        async with semaphore:
            try:
                # Detect transformer from OData context
                context = await get_odata_context(file_path)
                transformer_class = get_transformer_class_from_odata(context)
                transformer = transformer_class()
                print(f"Processing file: {os.path.basename(file_path)} with transformer: {transformer_class.__name__}")
                users = aread_json_stream(file_path)
            except Exception as e:
                # Record and skip files that fail to read
                file_errors.append((file_path, f"Read error: {e}"))
                return

            file_failed = False
            file_results = []

            while True:
                # Read users in chunks for parallel transformation
                chunk = []
                try:
                    async for user in users:
                        chunk.append(user)
                        if len(chunk) == chunk_size:
                            break
                except Exception as e:
                    # Record and skip files that fail to read a chunk
                    file_errors.append((file_path, f"Chunk read error: {e}"))
                    file_failed = True
                    break

                if not chunk:
                    break

                try:
                    # Transform users in parallel using asyncio and thread pool
                    loop = asyncio.get_running_loop()
                    chunk_results = await asyncio.gather(
                        *(loop.run_in_executor(None, transformer.transform, user) for user in chunk)
                    )
                    file_results.extend(chunk_results)
                except Exception as e:
                    # Record and skip files that fail to transform a chunk
                    file_errors.append((file_path, f"Chunk transform error: {e}"))
                    file_failed = True
                    break

            if not file_failed:
                # Collect transformed users from successfully processed files
                results.extend(file_results)

    async def transformed_users_generator():
        # Find all JSON files in the input directory
        json_files = glob.glob(os.path.join(input_directory, "*.json"))
        json_files.sort()
        print(f"Processing {len(json_files)} JSON files from {input_directory}")

        # Semaphore to limit concurrent file processing
        semaphore = asyncio.Semaphore(max_concurrent_files)
        results = []
        # Launch file processing tasks with concurrency control
        tasks = [process_file(file_path, semaphore, results) for file_path in json_files]
        await asyncio.gather(*tasks)
        # Yield all transformed users
        for result in results:
            yield result

    try:
        # Write transformed users in batches to output directory asynchronously
        await write_batches(output_dir, transformed_users_generator(), chunk_size)
    except Exception as e:
        raise e

    # Print any errors encountered during processing
    if file_errors:
        print("\nErrors occurred while processing files:")
        for file_path, error in file_errors:
            print(f"{file_path}: {error}")

if __name__ == "__main__":
    try:
        # Run the user processing pipeline asynchronously
        asyncio.run(process_users("usersapi", "transformed_users"))
    except Exception as e:
        print(f"Fatal error: {e}")