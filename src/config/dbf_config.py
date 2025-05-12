from dataclasses import dataclass
from pathlib import Path

@dataclass
class DBFConfig:
    """Configuration for DBF connection and reading."""
    dll_path: str
    encryption_password: str
    source_directory: str
    limit_rows: int = None  # Optional, set to None for no limit
    
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
