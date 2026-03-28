-- 1.1 Pagos huérfanos: ID_CLIENTE no existe en STG_CLIENTE
SELECT p.STG_ID_PAGO, p.STG_ID_CLIENTE
FROM   jardineria_staging.dbo.STG_PAGO p
LEFT JOIN jardineria_staging.dbo.STG_CLIENTE c ON p.STG_ID_CLIENTE = c.STG_ID_CLIENTE
WHERE  c.STG_ID_CLIENTE IS NULL;

-- 1.2 Pedidos huérfanos: ID_CLIENTE no existe en STG_CLIENTE
SELECT p.STG_ID_PEDIDO, p.STG_ID_CLIENTE
FROM   jardineria_staging.dbo.STG_PEDIDO p
LEFT JOIN jardineria_staging.dbo.STG_CLIENTE c ON p.STG_ID_CLIENTE = c.STG_ID_CLIENTE
WHERE  c.STG_ID_CLIENTE IS NULL;

-- 1.3 Detalles con pedido inexistente
SELECT d.STG_ID_DETALLE, d.STG_ID_PEDIDO
FROM   jardineria_staging.dbo.STG_DETALLE_PEDIDO d
LEFT JOIN jardineria_staging.dbo.STG_PEDIDO p ON d.STG_ID_PEDIDO = p.STG_ID_PEDIDO
WHERE  p.STG_ID_PEDIDO IS NULL;

-- 1.4 Detalles con producto inexistente
SELECT d.STG_ID_DETALLE, d.STG_ID_PRODUCTO
FROM   jardineria_staging.dbo.STG_DETALLE_PEDIDO d
LEFT JOIN jardineria_staging.dbo.STG_PRODUCTO pr ON d.STG_ID_PRODUCTO = pr.STG_ID_PRODUCTO
WHERE  pr.STG_ID_PRODUCTO IS NULL;

-- 1.5 Consistencia de datos desnormalizados (nombre cliente en PAGO vs CLIENTE)
SELECT p.STG_ID_PAGO, p.STG_ID_CLIENTE,
       p.STG_CLI_NOMBRE   AS nombre_en_pago,
       c.STG_NOMBRE_CLIENTE AS nombre_en_cliente
FROM   jardineria_staging.dbo.STG_PAGO p
JOIN   jardineria_staging.dbo.STG_CLIENTE c ON p.STG_ID_CLIENTE = c.STG_ID_CLIENTE
WHERE  p.STG_CLI_NOMBRE <> c.STG_NOMBRE_CLIENTE;

-- 1.6 Consistencia del nombre de producto en DETALLE_PEDIDO vs PRODUCTO
SELECT d.STG_ID_DETALLE,
       d.STG_PRO_NOMBRE   AS nombre_en_detalle,
       pr.STG_NOMBRE       AS nombre_en_producto
FROM   jardineria_staging.dbo.STG_DETALLE_PEDIDO d
JOIN   jardineria_staging.dbo.STG_PRODUCTO pr ON d.STG_ID_PRODUCTO = pr.STG_ID_PRODUCTO
WHERE  d.STG_PRO_NOMBRE <> pr.STG_NOMBRE;

-- 2.1 Nulos en campos obligatorios de STG_CLIENTE
SELECT STG_ID_CLIENTE,
       CASE WHEN STG_NOMBRE_CLIENTE IS NULL THEN 'NOMBRE_NULL ' ELSE '' END +
       CASE WHEN STG_CIUDAD          IS NULL THEN 'CIUDAD_NULL ' ELSE '' END +
       CASE WHEN STG_PAIS            IS NULL THEN 'PAIS_NULL '   ELSE '' END +
       CASE WHEN STG_LIMITE_CREDITO  IS NULL THEN 'LIMITE_NULL ' ELSE '' END AS problemas
FROM   jardineria_staging.dbo.STG_CLIENTE
WHERE  STG_NOMBRE_CLIENTE IS NULL
    OR STG_CIUDAD          IS NULL
    OR STG_PAIS            IS NULL
    OR STG_LIMITE_CREDITO  IS NULL;

-- 2.2 Nulos críticos en STG_PEDIDO
SELECT STG_ID_PEDIDO,
       CASE WHEN STG_ESTADO     IS NULL THEN 'ESTADO_NULL '      ELSE '' END +
       CASE WHEN STG_ID_CLIENTE IS NULL THEN 'CLIENTE_NULL '     ELSE '' END +
       CASE WHEN STG_CLI_NOMBRE IS NULL THEN 'CLI_NOMBRE_NULL '  ELSE '' END AS problemas
FROM   jardineria_staging.dbo.STG_PEDIDO
WHERE  STG_ESTADO IS NULL OR STG_ID_CLIENTE IS NULL OR STG_CLI_NOMBRE IS NULL;

-- 2.3 Productos sin precio de proveedor (puede ser válido, pero conviene auditarlo)
SELECT STG_ID_PRODUCTO, STG_NOMBRE, STG_PRECIO_VENTA
FROM   jardineria_staging.dbo.STG_PRODUCTO
WHERE  STG_PRECIO_PROVEEDOR IS NULL;

-- 2.4 Pedidos sin fecha de entrega (distinguir entregados vs pendientes)
SELECT STG_ID_PEDIDO, STG_ESTADO, STG_FECHA_PEDIDO, STG_FECHA_ESPERADA
FROM   jardineria_staging.dbo.STG_PEDIDO
WHERE  STG_FECHA_ENTREGA IS NULL AND STG_ESTADO = 'Entregado';

-- 3.1 Clientes duplicados por nombre (posible carga doble)
SELECT STG_NOMBRE_CLIENTE, COUNT(*) AS total
FROM   jardineria_staging.dbo.STG_CLIENTE
GROUP BY STG_NOMBRE_CLIENTE
HAVING COUNT(*) > 1
ORDER BY total DESC;

-- 3.2 Transacciones de pago duplicadas por cliente + fecha + total
SELECT STG_ID_CLIENTE, STG_FECHA_PAGO, STG_TOTAL, COUNT(*) AS total
FROM   jardineria_staging.dbo.STG_PAGO
GROUP BY STG_ID_CLIENTE, STG_FECHA_PAGO, STG_TOTAL
HAVING COUNT(*) > 1;

-- 3.3 Líneas de detalle duplicadas en un mismo pedido (mismo producto)
SELECT STG_ID_PEDIDO, STG_ID_PRODUCTO, COUNT(*) AS lineas
FROM   jardineria_staging.dbo.STG_DETALLE_PEDIDO
GROUP BY STG_ID_PEDIDO, STG_ID_PRODUCTO
HAVING COUNT(*) > 1;

-- 3.4 Productos con código duplicado (la UQ debería impedirlo, pero útil en pre-carga)
SELECT STG_CODIGO_PRODUCTO, COUNT(*) AS total
FROM   jardineria_staging.dbo.STG_PRODUCTO
GROUP BY STG_CODIGO_PRODUCTO
HAVING COUNT(*) > 1;

-- 4.1 Fechas fuera de rango lógico en PEDIDO
SELECT STG_ID_PEDIDO, STG_FECHA_PEDIDO, STG_FECHA_ESPERADA, STG_FECHA_ENTREGA
FROM   jardineria_staging.dbo.STG_PEDIDO
WHERE  STG_FECHA_ESPERADA < STG_FECHA_PEDIDO          -- esperada antes de pedido
    OR STG_FECHA_ENTREGA  < STG_FECHA_PEDIDO          -- entrega antes de pedido
    OR STG_FECHA_PEDIDO   > GETDATE();                -- fecha futura

-- 4.2 Pagos con fecha futura o muy antigua
SELECT STG_ID_PAGO, STG_FECHA_PAGO, STG_TOTAL
FROM   jardineria_staging.dbo.STG_PAGO
WHERE  STG_FECHA_PAGO > GETDATE()
    OR STG_FECHA_PAGO < '2000-01-01';

-- 4.3 Valores negativos o cero en precios y montos
SELECT STG_ID_PRODUCTO, STG_NOMBRE, STG_PRECIO_VENTA, STG_PRECIO_PROVEEDOR
FROM   jardineria_staging.dbo.STG_PRODUCTO
WHERE  STG_PRECIO_VENTA    <= 0
    OR STG_PRECIO_PROVEEDOR < 0;

SELECT STG_ID_PAGO, STG_TOTAL
FROM   jardineria_staging.dbo.STG_PAGO
WHERE  STG_TOTAL <= 0;

SELECT STG_ID_DETALLE, STG_CANTIDAD, STG_PRECIO_UNIDAD
FROM   jardineria_staging.dbo.STG_DETALLE_PEDIDO
WHERE  STG_CANTIDAD      <= 0
    OR STG_PRECIO_UNIDAD <= 0;

-- 4.4 Límite de crédito negativo en CLIENTE
SELECT STG_ID_CLIENTE, STG_NOMBRE_CLIENTE, STG_LIMITE_CREDITO
FROM   jardineria_staging.dbo.STG_CLIENTE
WHERE  STG_LIMITE_CREDITO < 0;

-- 4.5 Margen bruto negativo (precio venta < precio proveedor)
SELECT STG_ID_PRODUCTO, STG_NOMBRE, STG_PRECIO_VENTA,
       STG_PRECIO_PROVEEDOR, STG_MARGEN_BRUTO
FROM   jardineria_staging.dbo.STG_PRODUCTO
WHERE  STG_MARGEN_BRUTO < 0;

-- 4.6 Estado de pedido con valores fuera del catálogo esperado
SELECT STG_ESTADO, COUNT(*) AS total
FROM jardineria_staging.dbo.STG_PEDIDO
WHERE STG_ESTADO NOT IN ('ENTREGADO', 'PENDIENTE', 'RECHAZADO')
GROUP BY STG_ESTADO;

-- 5.1 Verificar STG_ANIO_PAGO y STG_MES_PAGO (columnas persistidas)
SELECT STG_ID_PAGO, STG_FECHA_PAGO, STG_ANIO_PAGO, STG_MES_PAGO
FROM   jardineria_staging.dbo.STG_PAGO
WHERE  STG_ANIO_PAGO <> YEAR(STG_FECHA_PAGO)
    OR STG_MES_PAGO  <> MONTH(STG_FECHA_PAGO);

-- 5.2 Verificar STG_DIAS_RETRASO (positivo = entregado tarde)
SELECT STG_ID_PEDIDO, STG_FECHA_ESPERADA, STG_FECHA_ENTREGA, STG_DIAS_RETRASO
FROM   jardineria_staging.dbo.STG_PEDIDO
WHERE  STG_FECHA_ENTREGA IS NOT NULL
  AND  STG_DIAS_RETRASO <> DATEDIFF(DAY, STG_FECHA_ESPERADA, STG_FECHA_ENTREGA);

-- 5.3 Verificar STG_IMPORTE_LINEA en detalle
SELECT STG_ID_DETALLE, STG_CANTIDAD, STG_PRECIO_UNIDAD, STG_IMPORTE_LINEA,
       STG_CANTIDAD * STG_PRECIO_UNIDAD AS importe_calculado
FROM   jardineria_staging.dbo.STG_DETALLE_PEDIDO
WHERE  STG_IMPORTE_LINEA <> STG_CANTIDAD * STG_PRECIO_UNIDAD;

SELECT 'Clientes totales'            AS metrica, COUNT(*)          AS valor FROM jardineria_staging.dbo.STG_CLIENTE
UNION ALL
SELECT 'Clientes sin país',            COUNT(*) FROM jardineria_staging.dbo.STG_CLIENTE WHERE STG_PAIS IS NULL
UNION ALL
SELECT 'Clientes sin límite crédito',  COUNT(*) FROM jardineria_staging.dbo.STG_CLIENTE WHERE STG_LIMITE_CREDITO IS NULL
UNION ALL
SELECT 'Pedidos totales',              COUNT(*) FROM jardineria_staging.dbo.STG_PEDIDO
UNION ALL
SELECT 'Pedidos con fecha inválida',   COUNT(*) FROM jardineria_staging.dbo.STG_PEDIDO WHERE STG_FECHA_ESPERADA < STG_FECHA_PEDIDO
UNION ALL
SELECT 'Pedidos entregados sin fecha', COUNT(*) FROM jardineria_staging.dbo.STG_PEDIDO WHERE STG_FECHA_ENTREGA IS NULL AND STG_ESTADO = 'ENTREGADO'
UNION ALL
SELECT 'Pagos con total <= 0',         COUNT(*) FROM jardineria_staging.dbo.STG_PAGO WHERE STG_TOTAL <= 0
UNION ALL
SELECT 'Productos margen negativo',    COUNT(*) FROM jardineria_staging.dbo.STG_PRODUCTO WHERE STG_MARGEN_BRUTO < 0
UNION ALL
SELECT 'Detalles cantidad <= 0',       COUNT(*) FROM jardineria_staging.dbo.STG_DETALLE_PEDIDO WHERE STG_CANTIDAD <= 0
ORDER BY 1;

SELECT STG_ESTADO, COUNT(*) AS total
FROM jardineria_staging.dbo.STG_PEDIDO
GROUP BY STG_ESTADO
ORDER BY total DESC;