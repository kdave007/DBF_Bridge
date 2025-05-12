import sys
from pathlib import Path
from datetime import datetime, timedelta
import json

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
            encryption_password="X3W",
            source_directory=source_dir,
            limit_rows=3  # Limit to 3 sales for testing
        )
        
        # Initialize mapping manager
        mapping_file = Path(project_root) / "mappings.json"
        mapping_manager = MappingManager(str(mapping_file))
        
        # Create controller
        controller = VentasController(mapping_manager, config)
        
        # Get data for last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        print(f"\nFetching sales from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print("=" * 50)
        
        # Get sales data with details
        data = controller.get_sales_in_range(start_date, end_date)
        print(f"\nFound {len(data)} sales")
        
        # Print first few records with their details
        print("\nFirst few sales with details:")
        for i, sale in enumerate(data[:2], 1):
            print(f"\nSale {i}:")
            print(json.dumps(sale, indent=2, ensure_ascii=False))
            
    except Exception as e:
        print(f"\nError: {str(e)}")
        raise

if __name__ == "__main__":
    main()
