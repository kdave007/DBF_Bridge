from typing import Optional, Dict, Any
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from pathlib import Path

class PostgresConnection:
    _connection_pool = None

    @staticmethod
    def get_db_config() -> Dict[str, str]:
        """
        Returns the database configuration in the format needed by other classes.
        Uses 'database' instead of 'dbname' for compatibility.
        
        Returns:
            Dict[str, str]: Database configuration dictionary
        """
        # Create a temporary instance to access the configuration
        temp_instance = PostgresConnection()
        
        # Return the configuration with the correct key names
        return {
            'host': temp_instance.db_config['host'],
            'database': temp_instance.db_config['dbname'],  # Convert dbname to database
            'user': temp_instance.db_config['user'],
            'password': temp_instance.db_config['password'],
            'port': temp_instance.db_config['port']
        }

    def __init__(self):
        """Initialize PostgreSQL connection with environment variables"""
        # Load environment variables
        env_path = Path(__file__).parent.parent.parent / '.env'
        load_dotenv(env_path)

        # Get database configuration from environment variables
        self.db_config = {
            'dbname': os.getenv('PG_DATABASE'),
            'user': os.getenv('PG_USER'),
            'password': os.getenv('PG_PASSWORD'),
            'host': os.getenv('PG_HOST'),
            'port': os.getenv('PG_PORT', '5432')
        }

        # Validate configuration
        missing_vars = [key for key, value in self.db_config.items() if value is None]
        if missing_vars:
            raise ValueError(f'Missing required PostgreSQL environment variables: {missing_vars}')

        # Initialize connection pool if not exists
        if PostgresConnection._connection_pool is None:
            self._initialize_connection_pool()

    def _initialize_connection_pool(self, minconn: int = 1, maxconn: int = 10) -> None:
        """Initialize the connection pool with min and max connections"""
        try:
            PostgresConnection._connection_pool = pool.SimpleConnectionPool(
                minconn,
                maxconn,
                **self.db_config,
                cursor_factory=RealDictCursor
            )
        except psycopg2.Error as e:
            raise Exception(f'Error initializing PostgreSQL connection pool: {e}')

    def get_connection(self) -> Any:
        """Get a connection from the pool"""
        if PostgresConnection._connection_pool is None:
            self._initialize_connection_pool()
        return PostgresConnection._connection_pool.getconn()

    def return_connection(self, connection: Any) -> None:
        """Return a connection to the pool"""
        PostgresConnection._connection_pool.putconn(connection)

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a query and return results as dictionary"""
        connection = None
        cursor = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            cursor.execute(query, params)
            
            if cursor.description:  # If the query returns results
                results = cursor.fetchall()
            else:
                results = {"affected_rows": cursor.rowcount}
            
            connection.commit()
            return results

        except psycopg2.Error as e:
            if connection:
                connection.rollback()
            raise Exception(f'Database error: {e}')

        finally:
            if cursor:
                cursor.close()
            if connection:
                self.return_connection(connection)
                
    def begin_transaction(self) -> Any:
        """Begin a new transaction and return the connection"""
        connection = self.get_connection()
        return connection

    def commit_transaction(self, connection: Any) -> None:
        """Commit the transaction and return the connection to the pool"""
        connection.commit()
        self.return_connection(connection)

    def rollback_transaction(self, connection: Any) -> None:
        """Rollback the transaction and return the connection to the pool"""
        connection.rollback()
        self.return_connection(connection)

    def execute_batch_update(self, query: str, params_list: list[Dict[str, Any]], connection: Any = None) -> Dict[str, Any]:
        """Execute the same query with different parameters in batch"""
        own_connection = connection is None
        cursor = None
        try:
            if own_connection:
                connection = self.get_connection()
            cursor = connection.cursor()
            
            # Using execute_batch for better performance
            from psycopg2.extras import execute_batch
            execute_batch(cursor, query, params_list)
            
            results = {"affected_rows": cursor.rowcount}
            
            if own_connection:
                connection.commit()
                self.return_connection(connection)
            
            return results

        except psycopg2.Error as e:
            if own_connection and connection:
                connection.rollback()
                self.return_connection(connection)
            raise Exception(f'Database error in batch operation: {e}')

        finally:
            if cursor:
                cursor.close()

    def close_pool(self) -> None:
        """Close the connection pool"""
        if PostgresConnection._connection_pool:
            PostgresConnection._connection_pool.closeall()
            PostgresConnection._connection_pool = None

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close_pool()
