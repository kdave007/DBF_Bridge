import os
import sys
from pathlib import Path

# Configurar correctamente el PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print(f"PYTHONPATH: {sys.path}")  # Debug

try:
    from src.controllers.find_matches_process import MatchesProcess
except ImportError as e:
    print(f"ImportError: {e}")
    raise

def main():
    print("=== Starting simple test for MatchesProcess ===")
    
    processor = MatchesProcess()
    
    print("Calling compare_batches()...")
    try:
        result = processor.compare_batches()
        if result:
            print("Test completed successfully!")
        else:
            print("Test completed with warnings")
        return result
    except Exception as e:
        print(f"Test failed with error: {str(e)}")
        return False

if __name__ == "__main__":
    main()
