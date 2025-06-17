import os
import sys
from pathlib import Path
from datetime import date

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
     # Let's try with the exact date from your screenshot: 20/03/2025
    start_date = date(2025, 6, 17)  # year month day
    end_date = date(2025, 6, 18)  # year month day

    process = WorkFlow()
    
    print("Calling compare_batches()...")

    url_source_A = r"C:\Users\gtdri\Documents\projects\care\DBF_Bridge\pospcp"
    url_dll_A = r"C:\Users\gtdri\Documents\projects\care\DBF_Bridge\Advantage.Data.Provider.dll"

    url_dll_B=r"C:\Users\campo\Documents\projects\DBF_Bridge\Advantage.Data.Provider.dll"
    # url_source_B=r"C:\Users\campo\Documents\projects\DBF_Bridge\pospcp"

    # url_source_B=r"C:\Users\campo\Documents\prueba_dbf"
    url_source_B=r"C:\PVSI"

    try:
        config = DBFConfig(
            dll_path=url_dll_B,
            encryption_password="X3WGTXG5QJZ6K9ZC4VO2",
            source_directory=url_source_B,
            limit_rows=500  # Limit to 3 sales for testing
        )
        result = process.start(config, start_date, end_date)
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
