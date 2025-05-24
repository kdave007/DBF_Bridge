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
    from src.controllers.main_workflow import WorkFlow
except ImportError as e:
    print(f"ImportError: {e}")
    raise

def main():
    print("=== Starting simple test for MatchesProcess ===")
    
    process = WorkFlow()
    
    print("Calling compare_batches()...")

    url_source_A = r"C:\Users\gtdri\Documents\projects\care\DBF_Bridge\pospcp"
    url_dll_A = r"C:\Users\gtdri\Documents\projects\care\DBF_Bridge\Advantage.Data.Provider.dll"

    url_dll_B=r"C:\Users\campo\Documents\projects\DBF_Bridge\Advantage.Data.Provider.dll"
    url_source_B=r"C:\Users\campo\Documents\projects\DBF_encrypted\pospcp"

    try:
        config = DBFConfig(
            dll_path=url_dll_A,
            encryption_password="X3WGTXG5QJZ6K9ZC4VO2",
            source_directory=url_source_A,
            limit_rows=500  # Limit to 3 sales for testing
        )
        result = process.start(config)
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
