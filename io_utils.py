import ijson
import json
import os
import aiofiles
import asyncio

def read_json_stream(path):
    """
    Reads JSON files with OData format.
    Streams users from the 'value' array without loading the entire file into memory.
    """
    with open(path, "r", encoding="utf-8") as f:
        # Use ijson to stream from the 'value' array in OData format
        for user in ijson.items(f, "value.item"):
            yield user

async def aread_json_stream(path):
    """
    Async wrapper for read_json_stream using a thread pool.
    Streams users from the 'value' array without loading the entire file into memory.
    """
    loop = asyncio.get_event_loop()
    def generator():
        with open(path, "r", encoding="utf-8") as f:
            for user in ijson.items(f, "value.item"):
                yield user
    for user in await loop.run_in_executor(None, lambda: list(generator())):
        yield user

async def write_json_stream(path, data_iterable):
    """
    Writes transformed users to a JSON file as a simple array.
    Streams data without loading everything into memory.
    """
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write("[\n")
        first = True
        async for item in data_iterable:
            if not first:
                await f.write(",\n")
            await f.write(json.dumps(item, ensure_ascii=False, indent=2))
            first = False
        await f.write("\n]")

async def write_batches(output_dir, data_iterable, batch_size=100):
    """
    Write batches of users to separate files asynchronously.
    """
    batch = []
    file_idx = 0
    async for user in data_iterable:
        batch.append(user)
        if len(batch) == batch_size:
            batch_path = os.path.join(output_dir, f"users_{file_idx:03d}.json")
            await write_json_stream(batch_path, _async_iter(batch))
            print(f"Wrote {len(batch)} users to {batch_path}")
            batch = []
            file_idx += 1
    if batch:
        batch_path = os.path.join(output_dir, f"users_{file_idx:03d}.json")
        await write_json_stream(batch_path, _async_iter(batch))
        print(f"Wrote {len(batch)} users to {batch_path}")

async def _async_iter(iterable):
    for item in iterable:
        yield item
