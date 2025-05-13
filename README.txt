# DBF Bridge - Instrucciones de Instalación y Uso

## Estructura de Archivos
Para ejecutar el programa, necesitas la siguiente estructura de archivos:

```
cualquier_carpeta/
│
├── main.exe              # El ejecutable principal
├── .env                 # Archivo de configuración
└── output/             # Carpeta donde se guardarán los archivos JSON
```

## Configuración
1. Crea un archivo `.env` con la siguiente información:
```
DBF_ENCRYPTION_PASSWORD=tu_contraseña_aquí
DBF_SOURCE_DIR=C:\ruta\a\tus\archivos\dbf
```

Notas importantes:
- La contraseña debe ser la misma que se usa para encriptar los archivos DBF
- La ruta de los archivos DBF debe ser la ubicación completa donde se encuentran los archivos en la PC

## Uso del Programa
1. Ejecuta `main.exe`
2. El programa te mostrará un menú con dos opciones:
   - Procesar archivos CAT_PROD
   - Procesar archivos VENTAS

3. Para CAT_PROD:
   - Te preguntará cuántos registros procesar
   - Ingresa 0 para procesar todos los registros
   - Ingresa un número específico para limitar la cantidad de registros

4. Para VENTAS:
   - Te pedirá un rango de fechas
   - Ingresa las fechas en el formato solicitado

5. Los archivos JSON resultantes se guardarán en la carpeta `output`

## Solución de Problemas
Si el programa no inicia:
1. Asegúrate de que el archivo `.env` existe y tiene el formato correcto
2. Verifica que la ruta de los archivos DBF en `.env` sea correcta
3. Confirma que la carpeta `output` existe

## Notas Técnicas
- El programa incluye internamente todos los archivos necesarios (DLL y mappings.json)
- No es necesario instalar ningún software adicional
- El programa debe tener permisos de lectura/escritura en su directorio

Para cualquier problema o consulta, contacta al equipo de soporte.
