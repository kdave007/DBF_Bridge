import os
from pathlib import Path
from datetime import datetime
import json
from dotenv import load_dotenv
from src.config.dbf_config import DBFConfig
from src.dbf_enc_reader.mapping_manager import MappingManager
from src.controllers.cat_prod_controller import CatProdController
from src.controllers.ventas_controller import VentasController

def get_resource_path(relative_path):
    """Get the path to a resource file, works for both script and exe"""
    if getattr(sys, 'frozen', False):
        # Running as exe
        base_path = Path(sys._MEIPASS)
    else:
        # Running as script
        base_path = Path(__file__).parent
    
    return base_path / relative_path

def get_base_path():
    """Get the base path for the application (works for both script and exe)"""
    if getattr(sys, 'frozen', False):
        # Running as exe
        return Path(sys.executable).parent
    else:
        # Running as script
        return Path(__file__).parent

def load_configuration():
    """Carga la configuración desde el archivo .env"""
    base_path = get_base_path()
    env_path = base_path / '.env'
    
    if not env_path.exists():
        print(f"\nError: No se encontró el archivo .env en: {env_path}")
        print("Por favor, asegúrese de que el archivo .env existe en el mismo directorio que el ejecutable.")
        exit(1)
        
    load_dotenv(env_path)
    
    # Verificar variables de entorno requeridas
    required_vars = ['DBF_ENCRYPTION_PASSWORD', 'DBF_SOURCE_DIR']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("\nError: Faltan las siguientes variables de entorno:")
        for var in missing_vars:
            print(f"- {var}")
        print("\nPor favor, crea un archivo .env basado en .env.example con los valores correctos.")
        exit(1)
    
    # Get DLL path from resources or environment
    dll_path = str(get_resource_path("Advantage.Data.Provider.dll"))
    
    return {
        'encryption_password': os.getenv('DBF_ENCRYPTION_PASSWORD'),
        'dll_path': dll_path,
        'source_dir': os.getenv('DBF_SOURCE_DIR')
    }

def get_record_limit():
    """Solicita al usuario el número de registros a procesar"""
    while True:
        try:
            limit = input("\n¿Cuántos registros desea procesar? (0 para todos): ")
            limit = int(limit)
            if limit < 0:
                print("\nError: El número debe ser mayor o igual a 0")
                continue
            return limit
        except ValueError:
            print("\nError: Por favor ingrese un número válido")

def get_date_range():
    """Solicita al usuario el rango de fechas"""
    while True:
        try:
            print("\nIngrese el rango de fechas (formato: DD/MM/YYYY)")
            start_date_str = input("Fecha inicial: ")
            end_date_str = input("Fecha final: ")
            
            start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
            end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
            
            if end_date < start_date:
                print("\nError: La fecha final debe ser posterior a la fecha inicial")
                continue
                
            return start_date, end_date
            
        except ValueError:
            print("\nError: Formato de fecha inválido. Use DD/MM/YYYY")

def save_output(data, filename):
    """Guarda los datos en un archivo JSON"""
    output_dir = get_base_path() / "output"
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"{filename}_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"\nDatos guardados en: {output_file}")

def main():
    try:
        # Cargar configuración
        config_data = load_configuration()
        
        # Verificar directorio fuente
        source_dir = config_data['source_dir']
        if not Path(source_dir).exists():
            raise ValueError(f"No se encontró el directorio fuente: {source_dir}")
        
        # Inicializar configuración
        config = DBFConfig(
            dll_path=config_data['dll_path'],
            encryption_password=config_data['encryption_password'],
            source_directory=source_dir,
            limit_rows=0  # Sin límite
        )
        
        # Initialize mapping manager
        mapping_file = get_resource_path("mappings.json")
        mapping_manager = MappingManager(str(mapping_file))
        
        # Menú principal
        while True:
            print("\n=== DBF Bridge ===")
            print("1. Procesar CAT_PROD")
            print("2. Procesar VENTAS")
            print("3. Salir")
            
            option = input("\nSeleccione una opción (1-3): ")
            
            if option == "1":
                # Procesar CAT_PROD
                limit = get_record_limit()
                config.limit_rows = limit  # Actualizar el límite en la configuración
                controller = CatProdController(mapping_manager, config)
                print(f"\nProcesando {'todos los' if limit == 0 else limit} registros de CAT_PROD...")
                data = controller.get_data_in_range()
                print(f"\nSe encontraron {len(data)} registros")
                save_output(data, "cat_prod")
                
            elif option == "2":
                # Procesar VENTAS
                controller = VentasController(mapping_manager, config)
                start_date, end_date = get_date_range()
                
                print(f"\nProcesando VENTAS del {start_date.strftime('%d/%m/%Y')} al {end_date.strftime('%d/%m/%Y')}...")
                data = controller.get_sales_in_range(start_date, end_date)
                print(f"\nSe encontraron {len(data)} registros")
                
                date_range = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
                save_output(data, f"ventas_{date_range}")
                
            elif option == "3":
                print("\n¡Hasta luego!")
                break
                
            else:
                print("\nOpción inválida. Por favor, seleccione 1, 2 o 3.")
                
    except Exception as e:
        print(f"\nError: {str(e)}")
        raise

if __name__ == "__main__":
    main()
