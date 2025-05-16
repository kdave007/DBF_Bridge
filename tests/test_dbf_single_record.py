import os
import sys
from pathlib import Path
import hashlib

# Set up project root
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.controllers.find_matches_process import MatchesProcess

def test_single_record_hashing():
    print("\n=== Testing Single Record Hashing ===")
    
    processor = MatchesProcess()
    
    # Use existing batch processing to get records
    batch_data = processor.compare_batches()
    
    if not batch_data:
        raise ValueError("No records found in batch processing")
    
    # Get first record from existing batch results
    first_record = batch_data[0]
    
    # Generate hash using existing method
    record_hash = processor.calculate_md5(first_record)
    
    # Validate hash
    assert len(record_hash) == 32, "MD5 hash should be 32 characters"
    assert record_hash == hashlib.md5(str(first_record).encode()).hexdigest(), \
        "Hash generation mismatch"
    
    print(f"Record Hash: {record_hash}")
    print("Single Record Hashing Test Passed!")

if __name__ == "__main__":
    test_single_record_hashing()
