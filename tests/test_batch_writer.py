import os
import shutil
import json
import unittest
import asyncio
from io_utils import write_batches

class TestBatchWrite(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_batch_output"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    async def test_batch_write_100_users(self):
        users = [{"id": i} for i in range(100)]
        async def user_gen():
            for user in users:
                yield user
        # this will not really await since unit tests run synchronously
        asyncio.run(write_batches(self.test_dir, user_gen(), batch_size=100))
        files = sorted(os.listdir(self.test_dir))
        self.assertEqual(len(files), 1)
        with open(os.path.join(self.test_dir, files[0]), "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(len(data), 100)
        self.assertEqual(data[0]["id"], 0)
        self.assertEqual(data[-1]["id"], 99)

    async def test_batch_write_250_users(self):
        users = [{"id": i} for i in range(250)]
        async def user_gen():
            for user in users:
                yield user
        # this will not really await since unit tests run synchronously
        asyncio.run(write_batches(self.test_dir, user_gen(), batch_size=100))
        files = sorted(os.listdir(self.test_dir))
        self.assertEqual(len(files), 3)
        counts = []
        for fname in files:
            with open(os.path.join(self.test_dir, fname), "r", encoding="utf-8") as f:
                data = json.load(f)
                counts.append(len(data))
        self.assertEqual(counts, [100, 100, 50])

    async def test_batch_write_less_than_batch(self):
        users = [{"id": i} for i in range(42)]
        async def user_gen():
            for user in users:
                yield user
        # this will not really await since unit tests run synchronously
        asyncio.run(write_batches(self.test_dir, user_gen(), batch_size=100))
        files = sorted(os.listdir(self.test_dir))
        self.assertEqual(len(files), 1)
        with open(os.path.join(self.test_dir, files[0]), "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(len(data), 42)

if __name__ == "__main__":
    unittest.main()