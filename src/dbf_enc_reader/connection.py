import clr
from pathlib import Path
from typing import Optional

class DBFConnection:
    _dll_loaded = False

    @classmethod
    def set_dll_path(cls, path: str) -> None:
        """Set the path to Advantage Data Provider DLL.
        
        Args:
            path: Full path to Advantage.Data.Provider.dll
        """
        try:
            clr.AddReference(path)
            cls._dll_loaded = True
        except Exception as e:
            raise RuntimeError(f"Failed to load Advantage DLL from {path}: {str(e)}")

    @classmethod
    def _check_dll_loaded(cls) -> None:
        """Check if DLL is loaded before attempting connection."""
        if not cls._dll_loaded:
            raise RuntimeError(
                "Advantage DLL path not set. Call DBFConnection.set_dll_path() first with the path to Advantage.Data.Provider.dll"
            )

    def __init__(self, data_source: str, encryption_password: str):
        """
        Initialize DBF connection.
        
        Args:
            data_source: Path to the DBF file
            encryption_password: Password for encrypted DBF
        """
        self.data_source = str(Path(data_source).resolve())
        self.connection_string = (
            f"data source={self.data_source}; "
            "ServerType=LOCAL; "
            "TableType=CDX; "
            "Shared=TRUE; "
            f"EncryptionPassword={encryption_password};"
        )
        print(f"Debug - Connection string: {self.connection_string}")
        self.conn: Optional[AdsConnection] = None
        self.reader = None



    def connect(self) -> None:
        """Establish connection to the DBF file."""
        self._check_dll_loaded()
        
        try:
            # Import here after DLL is loaded
            from Advantage.Data.Provider import AdsConnection
            from System import Exception as SystemException
            
            self.conn = AdsConnection(self.connection_string)
            self.conn.Open()
        except SystemException as e:
            raise ConnectionError(f"Failed to connect to DBF: {str(e)}")
        except ImportError as e:
            raise RuntimeError(f"Failed to import Advantage modules: {str(e)}. Make sure DLL is loaded correctly.")

    def get_reader(self, table_name: str, sql_query: str = None):
        """Get a reader for the specified table.
        
        Args:
            table_name: Name of the table to read
            sql_query: Optional SQL query to execute instead of reading whole table
            
        Returns:
            Data reader object
        """
        if not self.conn or not hasattr(self.conn, 'State') or self.conn.State != 'Open':
            self.connect()

        try:
            cmd = self.conn.CreateCommand()
            
            if sql_query:
                # Use SQL query
                print(f"\nExecuting SQL query: {sql_query}")
                cmd.CommandText = sql_query
            else:
                # Direct table access
                from System.Data import CommandType
                cmd.CommandText = table_name
                cmd.CommandType = CommandType.TableDirect
            
            self.reader = cmd.ExecuteReader()
            return self.reader
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {str(e)}")

    def close(self) -> None:
        """Close all connections and readers."""
        if self.reader:
            self.reader.Close()
        if self.conn and hasattr(self.conn, 'State'):
            try:
                from System.Data import ConnectionState
                if self.conn.State == ConnectionState.Open:
                    self.conn.Close()
            except ImportError:
                # Fallback if we can't import ConnectionState
                if self.conn.State == 'Open':
                    self.conn.Close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
