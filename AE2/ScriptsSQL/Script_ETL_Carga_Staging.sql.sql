
--Fase 3 — ETL: Extracción, Transformación y Carga

-- Limpiar el Staging (TRUNCATE en orden inverso)
USE jardineria_staging;


-- ── PASO 1: Deshabilitar todas las FK del Staging ─────────────────────────
ALTER TABLE dbo.STG_PAGO            NOCHECK CONSTRAINT ALL;
ALTER TABLE dbo.STG_DETALLE_PEDIDO  NOCHECK CONSTRAINT ALL;
ALTER TABLE dbo.STG_PEDIDO          NOCHECK CONSTRAINT ALL;
ALTER TABLE dbo.STG_PRODUCTO        NOCHECK CONSTRAINT ALL;
ALTER TABLE dbo.STG_CLIENTE         NOCHECK CONSTRAINT ALL;
ALTER TABLE dbo.STG_EMPLEADO        NOCHECK CONSTRAINT ALL;


-- ── PASO 2: Truncar en orden inverso de dependencias ─────────────────────
TRUNCATE TABLE dbo.STG_PAGO;
TRUNCATE TABLE dbo.STG_DETALLE_PEDIDO;
TRUNCATE TABLE dbo.STG_PEDIDO;
TRUNCATE TABLE dbo.STG_PRODUCTO;
TRUNCATE TABLE dbo.STG_CLIENTE;
TRUNCATE TABLE dbo.STG_EMPLEADO;


-- ── PASO 3: Rehabilitar todas las FK ─────────────────────────────────────
ALTER TABLE dbo.STG_PAGO            WITH CHECK CHECK CONSTRAINT ALL;
ALTER TABLE dbo.STG_DETALLE_PEDIDO  WITH CHECK CHECK CONSTRAINT ALL;
ALTER TABLE dbo.STG_PEDIDO          WITH CHECK CHECK CONSTRAINT ALL;
ALTER TABLE dbo.STG_PRODUCTO        WITH CHECK CHECK CONSTRAINT ALL;
ALTER TABLE dbo.STG_CLIENTE         WITH CHECK CHECK CONSTRAINT ALL;
ALTER TABLE dbo.STG_EMPLEADO        WITH CHECK CHECK CONSTRAINT ALL;

PRINT '>>> Staging limpiado. Listo para recarga.';

-- Carga de las 5 tablas
-- Paso 1 — STG_EMPLEADO  (JOIN a oficina para desnormalizar)
INSERT INTO dbo.STG_EMPLEADO (
    STG_ID_EMPLEADO, STG_NOMBRE_COMPLETO, STG_PUESTO,
    STG_OFI_CODIGO, STG_OFI_CIUDAD, STG_OFI_PAIS, STG_OFI_REGION,
    STG_FECHA_CARGA, STG_ORIGEN
)
SELECT
    e.ID_empleado,
    -- Consolidar nombre completo, eliminando apellido2 vacío:
    LTRIM(RTRIM(e.nombre + ' ' + e.apellido1
          + ISNULL(' ' + NULLIF(LTRIM(RTRIM(e.apellido2)),''), ''))),
    LTRIM(RTRIM(e.puesto)),
    -- Desnormalizar oficina:
    LTRIM(RTRIM(o.Descripcion)),
    UPPER(LTRIM(RTRIM(o.ciudad))),
    UPPER(LTRIM(RTRIM(o.pais))),
    NULLIF(LTRIM(RTRIM(o.region)), ''),
    GETDATE(), 'jardineria'
FROM jardineria.dbo.empleado e
LEFT JOIN jardineria.dbo.oficina o ON e.ID_oficina = o.ID_oficina;
PRINT '  Paso 1/5: STG_EMPLEADO — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' filas.';

-- Paso 2 — STG_CLIENTE  (JOIN a empleado + oficina para desnormalizar representante)
INSERT INTO dbo.STG_CLIENTE (
    STG_ID_CLIENTE, STG_NOMBRE_CLIENTE, STG_CIUDAD, STG_REGION,
    STG_PAIS, STG_LIMITE_CREDITO,
    STG_REP_NOMBRE, STG_REP_PUESTO, STG_REP_OFI_CIUDAD, STG_REP_OFI_PAIS,
    STG_FECHA_CARGA, STG_ORIGEN
)
SELECT
    c.ID_cliente,
    LTRIM(RTRIM(c.nombre_cliente)),
    UPPER(LTRIM(RTRIM(c.ciudad))),
    NULLIF(LTRIM(RTRIM(c.region)), ''),
    UPPER(NULLIF(LTRIM(RTRIM(c.pais)), '')),
    ISNULL(c.limite_credito, 0),
    -- Desnormalizar representante de ventas:
    LTRIM(RTRIM(e.nombre + ' ' + e.apellido1
          + ISNULL(' ' + NULLIF(LTRIM(RTRIM(e.apellido2)),''), ''))),
    LTRIM(RTRIM(e.puesto)),
    -- Desnormalizar oficina del representante:
    UPPER(LTRIM(RTRIM(o.ciudad))),
    UPPER(LTRIM(RTRIM(o.pais))),
    GETDATE(), 'jardineria'
FROM jardineria.dbo.cliente c
LEFT JOIN jardineria.dbo.empleado e ON c.ID_empleado_rep_ventas = e.ID_empleado
LEFT JOIN jardineria.dbo.oficina  o ON e.ID_oficina = o.ID_oficina;
PRINT '  Paso 2/5: STG_CLIENTE — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' filas.';

-- Paso 3 — STG_PRODUCTO  (JOIN a Categoria_producto para desnormalizar)

INSERT INTO dbo.STG_PRODUCTO (
    STG_ID_PRODUCTO, STG_CODIGO_PRODUCTO, STG_NOMBRE,
    STG_CATEGORIA, STG_PROVEEDOR, STG_CANTIDAD_STOCK,
    STG_PRECIO_VENTA, STG_PRECIO_PROVEEDOR,
    STG_FECHA_CARGA, STG_ORIGEN
)
SELECT
    p.ID_producto,
    UPPER(LTRIM(RTRIM(p.CodigoProducto))),
    LTRIM(RTRIM(p.nombre)),
    -- Desnormalizar nombre de categoría:
    LTRIM(RTRIM(cp.Desc_Categoria)),
    NULLIF(LTRIM(RTRIM(p.proveedor)), ''),
    ISNULL(p.cantidad_en_stock, 0),
    p.precio_venta,
    p.precio_proveedor,
    GETDATE(), 'jardineria'
FROM jardineria.dbo.producto p
LEFT JOIN jardineria.dbo.Categoria_producto cp ON p.Categoria = cp.Id_Categoria;
PRINT '  Paso 3/5: STG_PRODUCTO — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' filas.';

-- Paso 4 — STG_PEDIDO  (JOIN a cliente para desnormalizar datos geográficos)

INSERT INTO dbo.STG_PEDIDO (
    STG_ID_PEDIDO, STG_FECHA_PEDIDO, STG_FECHA_ESPERADA,
    STG_FECHA_ENTREGA, STG_ESTADO, STG_ID_CLIENTE,
    STG_CLI_NOMBRE, STG_CLI_CIUDAD, STG_CLI_PAIS,
    STG_FECHA_CARGA, STG_ORIGEN
)
SELECT
    p.ID_pedido,
    CAST(p.fecha_pedido   AS DATE),
    CAST(p.fecha_esperada AS DATE),
    CAST(p.fecha_entrega  AS DATE),         -- puede ser NULL
    UPPER(LTRIM(RTRIM(p.estado))),
    p.ID_cliente,
    -- Desnormalizar datos del cliente:
    LTRIM(RTRIM(c.nombre_cliente)),
    UPPER(LTRIM(RTRIM(c.ciudad))),
    UPPER(NULLIF(LTRIM(RTRIM(c.pais)), '')),
    GETDATE(), 'jardineria'
FROM jardineria.dbo.pedido p
LEFT JOIN jardineria.dbo.cliente c ON p.ID_cliente = c.ID_cliente;
PRINT '  Paso 4/5: STG_PEDIDO — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' filas.';

-- Paso 5 — STG_DETALLE_PEDIDO  (JOIN a producto + categoría para desnormalizar)

INSERT INTO dbo.STG_DETALLE_PEDIDO (
    STG_ID_PEDIDO, STG_ID_PRODUCTO, STG_CANTIDAD, STG_PRECIO_UNIDAD,
    STG_PRO_NOMBRE, STG_PRO_CATEGORIA,
    STG_FECHA_CARGA, STG_ORIGEN
)
SELECT
    dp.ID_pedido,
    dp.ID_producto,
    dp.cantidad,
    dp.precio_unidad,
    -- Desnormalizar nombre y categoría del producto:
    LTRIM(RTRIM(p.nombre)),
    LTRIM(RTRIM(cp.Desc_Categoria)),
    GETDATE(), 'jardineria'
FROM jardineria.dbo.detalle_pedido dp
LEFT JOIN jardineria.dbo.producto          p  ON dp.ID_producto = p.ID_producto
LEFT JOIN jardineria.dbo.Categoria_producto cp ON p.Categoria   = cp.Id_Categoria
WHERE dp.cantidad > 0 AND dp.precio_unidad > 0;
PRINT '  Paso 5/5: STG_DETALLE_PEDIDO — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' filas.';

-- Paso 6 — STG_PAGO  (JOIN a cliente para desnormalizar nombre)

INSERT INTO dbo.STG_PAGO (
    STG_ID_CLIENTE, STG_CLI_NOMBRE, STG_FORMA_PAGO,
    STG_ID_TRANSACCION, STG_FECHA_PAGO, STG_TOTAL,
    STG_FECHA_CARGA, STG_ORIGEN
)
SELECT
    pg.ID_cliente,
    -- Desnormalizar nombre del cliente:
    LTRIM(RTRIM(c.nombre_cliente)),
    LTRIM(RTRIM(pg.forma_pago)),
    LTRIM(RTRIM(pg.id_transaccion)),
    CAST(pg.fecha_pago AS DATE),
    pg.total,
    GETDATE(), 'jardineria'
FROM jardineria.dbo.pago pg
LEFT JOIN jardineria.dbo.cliente c ON pg.ID_cliente = c.ID_cliente
WHERE pg.total > 0;
PRINT '  STG_PAGO — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' filas.';

PRINT '>>> Fase 3 completada: ETL ejecutado correctamente.';

-- validacion de datos
-- 4.1 Conteo de Registros — Fuente vs. Staging (CORREGIDO)
SELECT
    Tabla,
    MAX(CASE WHEN Origen = 'FUENTE'  THEN Total ELSE 0 END) AS Filas_Fuente,
    MAX(CASE WHEN Origen = 'STAGING' THEN Total ELSE 0 END) AS Filas_Staging,
    MAX(CASE WHEN Origen = 'FUENTE'  THEN Total ELSE 0 END)
  - MAX(CASE WHEN Origen = 'STAGING' THEN Total ELSE 0 END) AS Diferencia
FROM (
    SELECT 'empleado'           AS Tabla, 'FUENTE'  AS Origen, COUNT(*) AS Total FROM jardineria.dbo.empleado            UNION ALL
    SELECT 'STG_EMPLEADO'       AS Tabla, 'STAGING' AS Origen, COUNT(*) AS Total FROM jardineria_staging.dbo.STG_EMPLEADO UNION ALL
    SELECT 'cliente'            AS Tabla, 'FUENTE'  AS Origen, COUNT(*) AS Total FROM jardineria.dbo.cliente              UNION ALL
    SELECT 'STG_CLIENTE'        AS Tabla, 'STAGING' AS Origen, COUNT(*) AS Total FROM jardineria_staging.dbo.STG_CLIENTE  UNION ALL
    SELECT 'producto'           AS Tabla, 'FUENTE'  AS Origen, COUNT(*) AS Total FROM jardineria.dbo.producto             UNION ALL
    SELECT 'STG_PRODUCTO'       AS Tabla, 'STAGING' AS Origen, COUNT(*) AS Total FROM jardineria_staging.dbo.STG_PRODUCTO UNION ALL
    SELECT 'pedido'             AS Tabla, 'FUENTE'  AS Origen, COUNT(*) AS Total FROM jardineria.dbo.pedido               UNION ALL
    SELECT 'STG_PEDIDO'         AS Tabla, 'STAGING' AS Origen, COUNT(*) AS Total FROM jardineria_staging.dbo.STG_PEDIDO   UNION ALL
    SELECT 'detalle_pedido'     AS Tabla, 'FUENTE'  AS Origen, COUNT(*) AS Total FROM jardineria.dbo.detalle_pedido               UNION ALL
    SELECT 'STG_DETALLE_PEDIDO' AS Tabla, 'STAGING' AS Origen, COUNT(*) AS Total FROM jardineria_staging.dbo.STG_DETALLE_PEDIDO   UNION ALL
    SELECT 'pago'               AS Tabla, 'FUENTE'  AS Origen, COUNT(*) AS Total FROM jardineria.dbo.pago                UNION ALL
    SELECT 'STG_PAGO'           AS Tabla, 'STAGING' AS Origen, COUNT(*) AS Total FROM jardineria_staging.dbo.STG_PAGO
) AS src
GROUP BY Tabla
ORDER BY Tabla;

SELECT 'STG_EMPLEADO — OFI_CIUDAD nula'       AS Problema, COUNT(*) AS Registros FROM dbo.STG_EMPLEADO       WHERE STG_OFI_CIUDAD IS NULL
UNION ALL
SELECT 'STG_CLIENTE — REP_NOMBRE nulo',                    COUNT(*) FROM dbo.STG_CLIENTE  WHERE STG_REP_NOMBRE IS NULL AND STG_LIMITE_CREDITO > 0
UNION ALL
SELECT 'STG_PRODUCTO — CATEGORIA nula',                    COUNT(*) FROM dbo.STG_PRODUCTO WHERE STG_CATEGORIA IS NULL
UNION ALL
SELECT 'STG_PEDIDO — CLI_NOMBRE nulo',                     COUNT(*) FROM dbo.STG_PEDIDO   WHERE STG_CLI_NOMBRE IS NULL
UNION ALL
SELECT 'STG_DETALLE — PRO_NOMBRE nulo',                    COUNT(*) FROM dbo.STG_DETALLE_PEDIDO WHERE STG_PRO_NOMBRE IS NULL
UNION ALL
SELECT 'STG_PAGO — CLI_NOMBRE nulo',                       COUNT(*) FROM dbo.STG_PAGO     WHERE STG_CLI_NOMBRE IS NULL;
-- Resultado esperado: 0 en todas las filas

SELECT 'FUENTE'  AS Origen, SUM(CAST(cantidad AS NUMERIC(15,2)) * precio_unidad) AS Total_Ventas
FROM jardineria.dbo.detalle_pedido WHERE cantidad > 0 AND precio_unidad > 0
UNION ALL
SELECT 'STAGING', SUM(STG_IMPORTE_LINEA) FROM jardineria_staging.dbo.STG_DETALLE_PEDIDO;

SELECT 'FUENTE'  AS Origen, SUM(total) AS Total_Pagos FROM jardineria.dbo.pago WHERE total > 0
UNION ALL
SELECT 'STAGING', SUM(STG_TOTAL) FROM jardineria_staging.dbo.STG_PAGO;
-- Diferencia esperada: 0 en ambos casos

SELECT COUNT(*) AS Inconsistencias_Nombre_Cliente
FROM dbo.STG_PEDIDO    p
JOIN dbo.STG_CLIENTE   c ON p.STG_ID_CLIENTE = c.STG_ID_CLIENTE
WHERE p.STG_CLI_NOMBRE <> c.STG_NOMBRE_CLIENTE;

-- Verificar que categoría en STG_DETALLE coincide con STG_PRODUCTO
SELECT COUNT(*) AS Inconsistencias_Categoria_Producto
FROM dbo.STG_DETALLE_PEDIDO d
JOIN dbo.STG_PRODUCTO       p ON d.STG_ID_PRODUCTO = p.STG_ID_PRODUCTO
WHERE d.STG_PRO_CATEGORIA <> p.STG_CATEGORIA;
-- Resultado esperado: 0 en ambas consultas

-- Consultas analiticas de negocio
--¿Qué oficina genera más ventas?
SELECT
    c.STG_REP_OFI_CIUDAD                             AS Ciudad_Oficina,
    c.STG_REP_OFI_PAIS                               AS Pais_Oficina,
    COUNT(DISTINCT p.STG_ID_PEDIDO)                  AS Total_Pedidos,
    COUNT(DISTINCT p.STG_ID_CLIENTE)                 AS Clientes_Atendidos,
    SUM(d.STG_IMPORTE_LINEA)                         AS Importe_Total_Ventas,
    RANK() OVER (ORDER BY SUM(d.STG_IMPORTE_LINEA) DESC) AS Ranking
FROM      dbo.STG_DETALLE_PEDIDO d
JOIN      dbo.STG_PEDIDO         p ON d.STG_ID_PEDIDO  = p.STG_ID_PEDIDO
JOIN      dbo.STG_CLIENTE        c ON p.STG_ID_CLIENTE = c.STG_ID_CLIENTE
GROUP BY  c.STG_REP_OFI_CIUDAD, c.STG_REP_OFI_PAIS
ORDER BY  Importe_Total_Ventas DESC;

--¿Qué categoría de producto prefiere cada cliente?
WITH RankedCategorias AS (
    SELECT
        p.STG_CLI_NOMBRE                            AS Cliente,
        d.STG_PRO_CATEGORIA                         AS Categoria,
        SUM(d.STG_CANTIDAD)                         AS Unidades,
        SUM(d.STG_IMPORTE_LINEA)                    AS Importe,
        ROW_NUMBER() OVER (
            PARTITION BY p.STG_ID_CLIENTE
            ORDER BY SUM(d.STG_IMPORTE_LINEA) DESC
        )                                           AS Rn
    FROM  dbo.STG_DETALLE_PEDIDO d
    JOIN  dbo.STG_PEDIDO         p ON d.STG_ID_PEDIDO = p.STG_ID_PEDIDO
    GROUP BY p.STG_ID_CLIENTE, p.STG_CLI_NOMBRE, d.STG_PRO_CATEGORIA
)
SELECT Cliente, Categoria AS Categoria_Favorita, Unidades, Importe
FROM   RankedCategorias
WHERE  Rn = 1
ORDER BY Importe DESC;

--Ranking global de categorías de producto
SELECT
    STG_PRO_CATEGORIA                               AS Categoria,
    COUNT(DISTINCT STG_ID_PEDIDO)                   AS Pedidos,
    SUM(STG_CANTIDAD)                               AS Unidades_Totales,
    SUM(STG_IMPORTE_LINEA)                          AS Importe_Total,
    ROUND(100.0 * SUM(STG_IMPORTE_LINEA)
        / SUM(SUM(STG_IMPORTE_LINEA)) OVER (), 2)  AS Pct_Sobre_Total
FROM  dbo.STG_DETALLE_PEDIDO
GROUP BY STG_PRO_CATEGORIA
ORDER BY Importe_Total DESC;

--Análisis de rentabilidad de productos — Top 10 por margen
SELECT TOP 10
    pro.STG_NOMBRE                                  AS Producto,
    pro.STG_CATEGORIA                               AS Categoria,
    pro.STG_PRECIO_VENTA                            AS Precio_Venta,
    pro.STG_PRECIO_PROVEEDOR                        AS Precio_Costo,
    pro.STG_MARGEN_BRUTO                            AS Margen_Bruto,
    ROUND(100.0 * pro.STG_MARGEN_BRUTO
        / pro.STG_PRECIO_VENTA, 2)                  AS Pct_Margen,
    SUM(det.STG_CANTIDAD)                           AS Unidades_Vendidas,
    SUM(det.STG_IMPORTE_LINEA)                      AS Ingreso_Total
FROM      dbo.STG_PRODUCTO        pro
LEFT JOIN dbo.STG_DETALLE_PEDIDO  det ON pro.STG_ID_PRODUCTO = det.STG_ID_PRODUCTO
GROUP BY  pro.STG_NOMBRE, pro.STG_CATEGORIA,
          pro.STG_PRECIO_VENTA, pro.STG_PRECIO_PROVEEDOR, pro.STG_MARGEN_BRUTO
ORDER BY  pro.STG_MARGEN_BRUTO DESC;

--Análisis temporal de ventas por año y mes
SELECT
    p.STG_ANIO_PEDIDO                               AS Anio,
    p.STG_MES_PEDIDO                                AS Mes,
    COUNT(DISTINCT p.STG_ID_PEDIDO)                 AS Pedidos,
    SUM(d.STG_IMPORTE_LINEA)                        AS Ventas_Mes,
    SUM(SUM(d.STG_IMPORTE_LINEA)) OVER (
        PARTITION BY p.STG_ANIO_PEDIDO
        ORDER BY p.STG_MES_PEDIDO
        ROWS UNBOUNDED PRECEDING
    )                                               AS Ventas_Acumuladas_Anio
FROM  dbo.STG_DETALLE_PEDIDO d
JOIN  dbo.STG_PEDIDO         p ON d.STG_ID_PEDIDO = p.STG_ID_PEDIDO
GROUP BY p.STG_ANIO_PEDIDO, p.STG_MES_PEDIDO
ORDER BY p.STG_ANIO_PEDIDO, p.STG_MES_PEDIDO;

--Desempeño por representante de ventas
SELECT
    c.STG_REP_NOMBRE                                AS Representante,
    c.STG_REP_OFI_CIUDAD                            AS Ciudad_Oficina,
    COUNT(DISTINCT p.STG_ID_PEDIDO)                 AS Pedidos_Gestionados,
    COUNT(DISTINCT p.STG_ID_CLIENTE)                AS Clientes_Activos,
    SUM(d.STG_IMPORTE_LINEA)                        AS Ventas_Totales,
    RANK() OVER (ORDER BY SUM(d.STG_IMPORTE_LINEA) DESC) AS Ranking_Ventas
FROM      dbo.STG_DETALLE_PEDIDO d
JOIN      dbo.STG_PEDIDO         p ON d.STG_ID_PEDIDO  = p.STG_ID_PEDIDO
JOIN      dbo.STG_CLIENTE        c ON p.STG_ID_CLIENTE = c.STG_ID_CLIENTE
WHERE     c.STG_REP_NOMBRE IS NOT NULL
GROUP BY  c.STG_REP_NOMBRE, c.STG_REP_OFI_CIUDAD
ORDER BY  Ventas_Totales DESC;




















