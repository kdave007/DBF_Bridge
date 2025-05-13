import sys
from pathlib import Path
from datetime import datetime, timedelta
import json
import time

# Add project root to path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from src.config.dbf_config import DBFConfig
from src.dbf_enc_reader.mapping_manager import MappingManager
from src.controllers.ventas_controller import VentasController

def main():
    try:
        # Initialize configuration
        source_dir = r"C:\Users\campo\Documents\projects\DBF_encrypted\pospcp"
        print(f"Checking source directory: {source_dir}")
        if not Path(source_dir).exists():
            raise ValueError(f"Source directory not found: {source_dir}")
            
            
        config = DBFConfig(
            dll_path=r"C:\Program Files (x86)\Advantage 10.10\ado.net\1.0\Advantage.Data.Provider.dll",
            encryption_password="X3WGTXG5QJZ6K9ZC4VO2",
            source_directory=source_dir,
            limit_rows=500  # Limit to 3 sales for testing
        )
        
        # Initialize mapping manager
        mapping_file = Path(project_root) / "mappings.json"
        mapping_manager = MappingManager(str(mapping_file))
        
        # Create controller
        controller = VentasController(mapping_manager, config)
        
        # Test date range (April 19-20, 2025)
        start_date = datetime(2025, 3, 20)# yyyy, dd, mm
        end_date = datetime(2025, 4, 21)
        
        print(f"\nFetching sales from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print("=" * 50)
        
        # Get sales data with details
        data = controller.get_sales_in_range(start_date, end_date)
        print(f"\nFound {len(data)} sales")
        
        # Create output directory if it doesn't exist
        output_dir = Path(project_root) / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        date_range = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
        filename = f"ventas_{date_range}_{timestamp}.json"
        output_file = output_dir / filename
        
        # Save data to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'date_range': {
                    'start': start_date.strftime('%Y-%m-%d'),
                    'end': end_date.strftime('%Y-%m-%d')
                },
                'total_sales': len(data),
                'sales': data
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\nData saved to: {output_file}")
            
    except Exception as e:
        print(f"\nError: {str(e)}")
        raise

if __name__ == "__main__":
    main()
