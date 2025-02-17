# s3-lock-file-processor
s3-lock-file-processor

![image](https://github.com/user-attachments/assets/ca13e354-91c6-4fcb-8c6d-069b8007c698)


### prerequisites 
```
pip install S3IncrementalProcessor
```

https://pypi.org/project/S3IncrementalProcessor/

Worker Code 
```
import boto3
import time
import json
import uuid
from functools import wraps
from datetime import datetime
from S3IncrementalProcessor import S3IncrementalProcessor

class S3Lock:
    def __init__(self, bucket_name, concurrency_limit, counter_name="active_locks.json"):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        self.concurrency_limit = concurrency_limit
        self.counter_name = counter_name
        self.counter_path = counter_name

    def lock(self, wait_time=10, retry_interval=1):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                lock_name = str(uuid.uuid4())
                lock_path = f"locks/{lock_name}"

                if self._acquire_lock(lock_name, lock_path, wait_time, retry_interval):
                    try:
                        print(f"[{datetime.now()}] Lock '{lock_name}' acquired. Starting work...")
                        result = func(*args, **kwargs)
                        return result
                    finally:
                        self._release_lock(lock_name, lock_path)
                        print(f"[{datetime.now()}] Lock '{lock_name}' released. Work completed.")
                else:
                    print(f"[{datetime.now()}] Could not acquire lock for job: {lock_name}, job will not run.")

            return wrapper
        return decorator

    def _acquire_lock(self, lock_name, lock_path, wait_time, retry_interval):
        start_time = time.time()
        while time.time() - start_time < wait_time:
            if self._check_concurrency_limit():
                try:
                    self.s3.put_object(Bucket=self.bucket_name, Key=lock_path, Body='')
                    self._increment_active_locks()
                    print(f"Lock '{lock_name}' acquired.")
                    return True
                except Exception:
                    pass  # Lock is already held; retry
            time.sleep(retry_interval)

        print(f"Failed to acquire lock '{lock_name}' after waiting {wait_time} seconds.")
        return False

    def _release_lock(self, lock_name, lock_path):
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=lock_path)
            self._decrement_active_locks()
            print(f"Lock '{lock_name}' released.")
        except Exception as e:
            print(f"Error releasing lock: {e}")

    def _check_concurrency_limit(self):
        active_locks = self._get_active_locks()
        return active_locks < self.concurrency_limit

    def _get_active_locks(self):
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=self.counter_path)
            content = response['Body'].read().decode('utf-8')
            return int(json.loads(content)['count'])
        except Exception:
            return 0

    def _increment_active_locks(self):
        active_locks = self._get_active_locks()
        new_count = active_locks + 1
        self._update_active_locks(new_count)

    def _decrement_active_locks(self):
        active_locks = self._get_active_locks()
        new_count = max(0, active_locks - 1)
        self._update_active_locks(new_count)

    def _update_active_locks(self, count):
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=self.counter_path, Body=json.dumps({'count': count}))
        except Exception as e:
            print(f"Error updating active lock count: {e}")

# Initialize the S3Lock
BUCKET_NAME = "<bucketname>"  # Replace with your S3 bucket name
CONCURRENCY_LIMIT = 1  # Allow up to 1 concurrent job
s3_lock = S3Lock(BUCKET_NAME, CONCURRENCY_LIMIT)

@s3_lock.lock(wait_time=30, retry_interval=2)
def worker():
    print("Doing some work...")
    processor = S3IncrementalProcessor(
        "s3://<bucketname>/raw/15/",
        "s3://<bucketname>/checkpoints/checkpoint.json"
    )

    # Fetch new files in batches
    new_files = processor.get_new_files(batch_size=5)

    if new_files:
        print(f"Processing {len(new_files)} files:")
        for file in new_files:
            print(f"- {file}")
            time.sleep(2)
        # Commit the checkpoint after processing
        processor.commit_checkpoint()
    else:
        print("No new or modified files found.")

    print("Work finished.")

# Main function
if __name__ == "__main__":
    worker()

```
