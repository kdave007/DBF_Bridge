-- Create factura_venta table
CREATE TABLE IF NOT EXISTS factura_venta (
    id VARCHAR(50) PRIMARY KEY,  -- Format: FA_folio
    cabecera VARCHAR(10),
    folio INTEGER,
    cliente VARCHAR(50),
    empleado INTEGER,
    fecha TIMESTAMP,
    total_bruto NUMERIC(10,2),
    detalles JSONB,  -- Store full JSON for reference
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    api_response JSONB,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create detalle_factura_venta table
CREATE TABLE IF NOT EXISTS detalle_factura_venta (
    id VARCHAR(50) PRIMARY KEY,  -- Format: FA_folio_REF
    header_id VARCHAR(50) REFERENCES factura_venta(id),
    folio INTEGER,
    referencia INTEGER,
    cantidad NUMERIC(10,3),
    precio NUMERIC(12,2),
    descuento NUMERIC(10,2),
    total_linea NUMERIC(12,2),
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    api_response JSONB,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
