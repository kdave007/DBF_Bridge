-- Create factura_venta table
CREATE TABLE IF NOT EXISTS factura_venta (
    id INTEGER PRIMARY KEY,
    cabecera CHARACTER VARYING(10),
    folio INTEGER,
    cliente CHARACTER VARYING(50),
    empleado INTEGER,
    fecha TIMESTAMP WITHOUT TIME ZONE,
    total_bruto NUMERIC(10,2),
    fecha_creacion TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create detalle_factura_venta table
CREATE TABLE IF NOT EXISTS detalle_factura_venta (
    id VARCHAR(50) PRIMARY KEY,  -- Format: folio_REF
    header_id VARCHAR(50),
    folio INTEGER,
    referencia INTEGER,
    cantidad NUMERIC(10,3),
    precio NUMERIC(12,2),
    descuento NUMERIC(10,2),
    total_linea NUMERIC(12,2),
    fecha_creacion TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create estado_factura_venta table
CREATE TABLE IF NOT EXISTS estado_factura_venta (
    id NUMERIC PRIMARY KEY,  -- Using folio as ID
    folio CHARACTER VARYING,
    total_partidas NUMERIC,
    descripcion CHARACTER VARYING,
    hash CHARACTER VARYING,
    estado CHARACTER VARYING(20),
    fecha_procesamiento TIMESTAMP WITH TIME ZONE,
    id_lote CHARACTER VARYING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
