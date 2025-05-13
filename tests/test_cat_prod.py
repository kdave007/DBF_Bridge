import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
import json

# Add project root to path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from src.config.dbf_config import DBFConfig
from src.dbf_enc_reader.mapping_manager import MappingManager
from src.controllers.cat_prod_controller import CatProdController

def main():
    try:
        # Initialize configuration
        source_dir = r"C:\Users\campo\Documents\projects\DBF_encrypted\pospcp"
        print(f"\nChecking source directory: {source_dir}")
        if not Path(source_dir).exists():
            raise ValueError(f"Source directory not found: {source_dir}")
            
            
        config = DBFConfig(
            dll_path=r"C:\Program Files (x86)\Advantage 10.10\ado.net\1.0\Advantage.Data.Provider.dll",
            encryption_password="X3WGTXG5QJZ6K9ZC4VO2",
            source_directory=source_dir,
            limit_rows=3
        )
        
        # Initialize mapping manager
        mapping_file = Path(project_root) / "mappings.json"
        mapping_manager = MappingManager(str(mapping_file))
        
        # Create controller
        controller = CatProdController(mapping_manager, config)
        
        print(f"\nFetching last {config.limit_rows} records from CAT_PROD")
        print("=" * 50)
        
        # Get data (using any date since we're ignoring the filter)
        data = controller.get_data_in_range()
        print(f"\nFound {len(data)} records")
        #Print results
        print(f"\nFound {len(data)} records")
        print("\nFirst few records:")
        for i, record in enumerate(data[:2], 1):
            print(f"\nRecord {i}:")
            print(json.dumps(record, indent=2, ensure_ascii=False))

        # Create output directory if it doesn't exist
        output_dir = Path(project_root) / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"cat_prod_{timestamp}.json"
        output_file = output_dir / filename    


        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'total': len(data),
                'results': data
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\nData saved to: {output_file}")    
            
    except Exception as e:
        print(f"\nError: {str(e)}")
        raise

if __name__ == "__main__":
    main()
