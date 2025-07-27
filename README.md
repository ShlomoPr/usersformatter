# User Data Transformation

## How to Run

1. (Optional) Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install requirements:
   ```bash
   pip install ijson aiofiles
   ```

3. Ensure your JSON files are in the `usersapi` directory. The application will automatically process all `.json` files in this directory.

4. Run the transformation (now uses asyncio):
   ```bash
   python main.py
   ```

5. Output will be in the `transformed_users` directory, with each JSON file containing 100 transformed users (e.g., `users_000.json`, `users_001.json`, ...).

## Input Data Format

The application expects JSON files in Microsoft Graph API OData format:

```json
{
    "@odata.context": "https://graph.microsoft.com/beta/$metadata#users(...)",
    "value": [
        {
            "userPrincipalName": "user@example.com",
            "usageLocation": "US",
            "mail": "user@example.com"
            // ... other user fields
        }
    ]
}
```

## Configuration

You can customize the processing behavior by modifying the parameters in `main.py`:

- **input_directory**: Change the directory containing input JSON files (default: "usersapi")
- **chunk_size**: Adjust the number of users processed in each parallel batch (default: 100)
- **output_dir**: Change the output directory name (default: "transformed_users"). Each file in this directory will contain 100 users.
- **max_concurrent_files**: Limit the number of files processed in parallel (default: 2). This is important for systems with limited CPU cores or memory, as processing too many files at once can overwhelm the machine and degrade performance. Adjust this value based on your system's capabilities.

```python
# Example usage
import asyncio
asyncio.run(process_users("usersapi", "transformed_users", chunk_size=50, max_concurrent_files=4))
```

## File Processing

The application will:

1. **Discover Files**: Automatically find all `.json` files in the specified input directory
2. **Process in Order**: Files are processed in alphabetical order for consistency
3. **Stream Processing**: Each file is processed individually using streaming to maintain memory efficiency
4. **Batch Output**: Transformed users are written in batches of 100 to separate JSON files in the output directory. This makes downstream database ingestion easier and more scalable.

## Architectural Decisions

- **Memory-Efficient Processing**: Uses chunked processing to handle large datasets without loading everything into memory at once.
- **Multi-File Support**: Processes multiple JSON files from a directory automatically
- **Streaming Architecture**: 
  - **Input**: Uses `ijson` to stream JSON data from OData format files, processing one user at a time (with async wrapper for compatibility)
  - **Processing**: Processes users in configurable chunks with parallel execution within each file using `asyncio`
  - **Output**: Streams transformed results directly to multiple batch files (100 users per file) in the output directory, without intermediate storage, using async IO
- **Concurrency**: 
  - The number of concurrently processed files is limited by `max_concurrent_files` using an `asyncio.Semaphore`. This prevents overloading systems with limited CPU or memory.
  - Within each file, user transformation is parallelized using `asyncio.get_running_loop().run_in_executor`, which offloads CPU-bound transformation tasks to threads managed by Python's default thread pool. This replaces the previous use of `ThreadPoolExecutor` and is fully integrated with the asyncio event loop.
- **Modular Design**: Separation of concerns between reading, transforming, and writing data for easy extensibility.
- **Async IO**: All file operations are performed asynchronously for maximum performance.

## Data Transformation Decisions

### signInActivity Modeling

The `signInActivity` field from Microsoft Graph API is transformed from a flat structure to a nested, organized structure:

**Original Microsoft Graph Structure:**
```json
"signInActivity": {
    "lastSignInDateTime": "1999-01-01T14:10:38",
    "lastSignInRequestId": "9d0630df-c6f4-4ea6-b236-4d716f7b3ab3",
    "lastNonInteractiveSignInDateTime": "2007-07-12T18:57:09",
    "lastNonInteractiveSignInRequestId": "767d68a2-8b8d-4812-8ea5-10581a9a83c7",
    "lastSuccessfulSignInDateTime": "1976-05-09T13:40:54",
    "lastSuccessfulSignInRequestId": "a8291692-0665-4f08-8159-52f717efec52"
}
```

**Transformed Structure:**
```json
"signInActivity": {
    "lastSignIn": {
        "dateTime": "1999-01-01T14:10:38",
        "requestId": "9d0630df-c6f4-4ea6-b236-4d716f7b3ab3"
    },
    "lastNonInteractiveSignIn": {
        "dateTime": "2007-07-12T18:57:09",
        "requestId": "767d68a2-8b8d-4812-8ea5-10581a9a83c7"
    },
    "lastSuccessfulSignIn": {
        "dateTime": "1976-05-09T13:40:54",
        "requestId": "a8291692-0665-4f08-8159-52f717efec52"
    }
}
```

**Benefits of this approach:**

1. **Logical Grouping**: Related fields (dateTime + requestId) are grouped together
2. **Clear Semantics**: Each sign-in type is clearly identified and separated
3. **Easy Access**: Applications can easily access specific sign-in types: `user.signInActivity.lastSignIn.dateTime`
4. **Maintainability**: Structure is self-documenting and easier to understand
5. **Extensibility**: Easy to add new fields to each sign-in type in the future
6. **Type Safety**: Better for strongly-typed systems and API contracts

### Field Mapping

The transformer also handles proper field mapping from Microsoft Graph API names to more standardized names:

- `userType` → `type`
- `usageLocation` → `location` 
- `accountEnabled` → `is_enabled`
- `givenName` → `first_name`
- `surname` → `last_name`

### OData Format Handling

The application automatically handles Microsoft Graph API OData format:

- **Input**: Reads from the `value` array within OData wrapped JSON files using streaming and async IO
- **Output**: Produces a clean JSON array without OData metadata
- **Streaming**: Processes the `value` array incrementally without loading entire files

## Performance Characteristics

- **Memory Usage**: Constant memory usage regardless of input file size or number of files (controlled by chunk_size)
- **Processing Speed**: Parallel processing within chunks for optimal CPU utilization using asyncio and thread pool offloading
- **Scalability**: Can handle arbitrarily large input files and multiple files without memory constraints
- **Multi-File Efficiency**: Processes files sequentially while maintaining memory efficiency within each file

## Database Ingestion Benefits

- **Parallel Import:** Smaller batch files allow for parallel ingestion into databases, improving speed and reliability.
- **Error Isolation:** If a batch fails, only the corresponding file is affected, not the entire dataset.
- **Scalability:** Large user datasets are split into manageable chunks, reducing memory and processing overhead for ETL and database import jobs.

## How to Add More Transformation Logic

To add more transformation logic:
1. Create a new transformer class that inherits from `BaseTransformer` in `transformer.py`.
2. Implement the `transform(self, user)` method.
3. Pass your transformer instance to the processing pipeline.

Example:
```python
from transformer import BaseTransformer

class CustomTransformer(BaseTransformer):
    def transform(self, user):
        # Custom transformation logic
        return {...}

# Use your transformer in the pipeline
transformer = CustomTransformer()
```

## Memory Efficiency Details

The current implementation uses a three-stage streaming pipeline for each file:

1. **Read Stream**: `aread_json_stream()` yields users one at a time from each OData JSON file using async IO and streaming
2. **Transform Stream**: Users are processed in chunks with parallel execution using asyncio and thread pool offloading, then yielded individually
3. **Write Stream**: `write_json_stream()` consumes the transformed users and writes them directly to the output file using async IO

This approach ensures that memory usage remains constant regardless of input file size or number of files, making it suitable for processing very large datasets distributed across multiple files.