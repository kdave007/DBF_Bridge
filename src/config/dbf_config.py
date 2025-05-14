from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv

@dataclass
class DBFConfig:
    """Configuration for DBF connection and reading."""
    dll_path: str = None
    encryption_password: str = None
    source_directory: str = None
    limit_rows: int = None  # Optional, set to None for no limit
    
    def __init__(self, dll_path=None, encryption_password=None, source_directory=None, limit_rows=None):
        # Load from .env if values not provided
        load_dotenv()
        
        self.dll_path = dll_path or os.getenv('DBF_DLL_PATH')
        self.encryption_password = encryption_password or os.getenv('DBF_ENCRYPTION_PASSWORD')
        self.source_directory = source_directory or os.getenv('DBF_SOURCE_DIR')
        self.limit_rows = limit_rows
        
        # Validate required fields
        if not self.dll_path:
            raise ValueError("dll_path is required. Set it directly or via DBF_DLL_PATH in .env")
        if not self.encryption_password:
            raise ValueError("encryption_password is required. Set it directly or via DBF_ENCRYPTION_PASSWORD in .env")
        if not self.source_directory:
            raise ValueError("source_directory is required. Set it directly or via DBF_SOURCE_DIR in .env")
    
    def __post_init__(self):
        """Validate and convert paths after initialization."""
        self.dll_path = str(Path(self.dll_path).resolve())
        self.source_directory = str(Path(self.source_directory).resolve())
        
    def get_table_path(self, table_name: str) -> str:
        """Get the full path for a DBF table.
        
        Args:
            table_name: Name of the DBF table/file
            
        Returns:
            Full path to the DBF file
        """
        return str(Path(self.source_directory) / table_name)
