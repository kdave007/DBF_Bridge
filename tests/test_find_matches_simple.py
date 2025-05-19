import os
import sys
from pathlib import Path

# Configurar correctamente el PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Now we can import from src
from src.config.dbf_config import DBFConfig

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
        config = DBFConfig(
            dll_path=r"C:\\Program Files (x86)\\Advantage 10.10\\ado.net\\1.0\\Advantage.Data.Provider.dll",
            encryption_password="X3WGTXG5QJZ6K9ZC4VO2",
            source_directory=r"C:\\Users\\campo\\Documents\\projects\\DBF_encrypted\\pospcp",
            limit_rows=500  # Limit to 3 sales for testing
        )
        result = processor.compare_batches(config)
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
