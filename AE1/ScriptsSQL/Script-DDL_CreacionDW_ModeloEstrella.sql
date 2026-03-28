-- ============================================================
--  DATA WAREHOUSE - MODELO ESTRELLA (OPTIMIZADO)
--  Solo hechos de venta | Atributos analíticamente relevantes
--  Motor: SQL Server
--  Fecha: 2026-03-05
-- ============================================================

-- ============================================================
--  CREACIÓN DE LA BASE DE DATOS
-- ============================================================
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DW_Ventas')
BEGIN
    CREATE DATABASE DW_Ventas
    COLLATE Modern_Spanish_CI_AS;
END
GO

USE DW_Ventas;
GO

-- ============================================================
--  LIMPIEZA DE OBJETOS PREVIOS
-- ============================================================
IF OBJECT_ID('dbo.vw_Ventas_Detalle',        'V') IS NOT NULL DROP VIEW dbo.vw_Ventas_Detalle;
IF OBJECT_ID('dbo.vw_Ventas_Mes_Categoria',  'V') IS NOT NULL DROP VIEW dbo.vw_Ventas_Mes_Categoria;
IF OBJECT_ID('dbo.sp_Cargar_Dim_Tiempo',     'P') IS NOT NULL DROP PROCEDURE dbo.sp_Cargar_Dim_Tiempo;
IF OBJECT_ID('dbo.FACT_Ventas',              'U') IS NOT NULL DROP TABLE dbo.FACT_Ventas;
IF OBJECT_ID('dbo.Dim_Tiempo',              'U') IS NOT NULL DROP TABLE dbo.Dim_Tiempo;
IF OBJECT_ID('dbo.Dim_Cliente',             'U') IS NOT NULL DROP TABLE dbo.Dim_Cliente;
IF OBJECT_ID('dbo.Dim_Producto',            'U') IS NOT NULL DROP TABLE dbo.Dim_Producto;
IF OBJECT_ID('dbo.Dim_Empleado',            'U') IS NOT NULL DROP TABLE dbo.Dim_Empleado;
IF OBJECT_ID('dbo.Dim_Oficina',             'U') IS NOT NULL DROP TABLE dbo.Dim_Oficina;
IF OBJECT_ID('dbo.Dim_Pedido',              'U') IS NOT NULL DROP TABLE dbo.Dim_Pedido;
GO


-- ============================================================
--  DIMENSIONES
-- ============================================================

-- ------------------------------------------------------------
--  DIM_TIEMPO
--  Campos eliminados: ninguno (es una dimensión técnica pura,
--  todos sus atributos tienen uso analítico directo)
-- ------------------------------------------------------------
CREATE TABLE dbo.Dim_Tiempo (
    ID_tiempo        INT         NOT NULL,   -- Surrogate key formato YYYYMMDD
    fecha            DATE        NOT NULL,
    anio             SMALLINT    NOT NULL,
    trimestre        TINYINT     NOT NULL,   -- 1-4
    mes              TINYINT     NOT NULL,   -- 1-12
    nombre_mes       VARCHAR(20) NOT NULL,
    semana_anio      TINYINT     NOT NULL,   -- Semana ISO (1-53)
    dia_mes          TINYINT     NOT NULL,
    nombre_dia       VARCHAR(20) NOT NULL,
    es_fin_semana    BIT         NOT NULL DEFAULT 0,
    nombre_trimestre AS ('T' + CAST(trimestre AS VARCHAR(1))
                         + '-' + CAST(anio AS VARCHAR(4))) PERSISTED,
    CONSTRAINT PK_Dim_Tiempo PRIMARY KEY (ID_tiempo)
);
GO

-- ------------------------------------------------------------
--  DIM_OFICINA
--  Eliminados: telefono, linea_direccion1, linea_direccion2,
--              codigo_postal  →  no aportan al análisis de ventas
-- ------------------------------------------------------------
CREATE TABLE dbo.Dim_Oficina (
    ID_oficina_sk  INT         IDENTITY(1,1) NOT NULL,
    ID_oficina_bk  INT         NOT NULL,
    descripcion    VARCHAR(10) NOT NULL,
    ciudad         VARCHAR(30) NOT NULL,
    pais           VARCHAR(50) NOT NULL,
    region         VARCHAR(50) NULL,
    fecha_carga    DATETIME    NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_Dim_Oficina PRIMARY KEY (ID_oficina_sk)
);
GO

-- ------------------------------------------------------------
--  DIM_EMPLEADO
--  Eliminados: apellido2, extension, email
--              → datos de contacto operativo sin valor analítico
-- ------------------------------------------------------------
CREATE TABLE dbo.Dim_Empleado (
    ID_empleado_sk  INT          IDENTITY(1,1) NOT NULL,
    ID_empleado_bk  INT          NOT NULL,
    nombre_completo AS (nombre + ' ' + apellido1) PERSISTED,
    nombre          VARCHAR(50)  NOT NULL,
    apellido1       VARCHAR(50)  NOT NULL,
    puesto          VARCHAR(50)  NULL,
    -- Jerarquía desnormalizada: evita joins adicionales en consultas
    nombre_jefe     VARCHAR(100) NULL,
    puesto_jefe     VARCHAR(50)  NULL,
    fecha_carga     DATETIME     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_Dim_Empleado PRIMARY KEY (ID_empleado_sk)
);
GO

-- ------------------------------------------------------------
--  DIM_CLIENTE
--  Eliminados: nombre_contacto, apellido_contacto, telefono, fax,
--              linea_direccion1, linea_direccion2, codigo_postal
--              → datos operativos de contacto sin uso analítico
--  Conservado: limite_credito  → permite segmentación por nivel de cliente
--  SCD Tipo 2: se versiona si cambia ciudad, región, país o límite de crédito
-- ------------------------------------------------------------
CREATE TABLE dbo.Dim_Cliente (
    ID_cliente_sk    INT           IDENTITY(1,1) NOT NULL,
    ID_cliente_bk    INT           NOT NULL,
    nombre_cliente   VARCHAR(50)   NOT NULL,
    ciudad           VARCHAR(50)   NOT NULL,
    region           VARCHAR(50)   NULL,
    pais             VARCHAR(50)   NULL,
    limite_credito   NUMERIC(15,2) NULL,
    -- Versionado SCD Tipo 2
    fecha_inicio     DATE          NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    fecha_fin        DATE          NULL,      -- NULL = registro actualmente vigente
    es_vigente       BIT           NOT NULL DEFAULT 1,
    fecha_carga      DATETIME      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_Dim_Cliente PRIMARY KEY (ID_cliente_sk)
);
GO

-- ------------------------------------------------------------
--  DIM_PRODUCTO
--  Eliminados: descripcion (texto libre sin valor analítico)
--  Categoría desnormalizada: evita snowflake y simplifica consultas
-- ------------------------------------------------------------
CREATE TABLE dbo.Dim_Producto (
    ID_producto_sk   INT           IDENTITY(1,1) NOT NULL,
    ID_producto_bk   INT           NOT NULL,
    codigo_producto  VARCHAR(15)   NOT NULL,
    nombre_producto  VARCHAR(70)   NOT NULL,
    proveedor        VARCHAR(50)   NULL,
    dimensiones      VARCHAR(25)   NULL,
    precio_venta_ref NUMERIC(15,2) NOT NULL,   -- Precio de referencia en catálogo
    -- Categoría desnormalizada
    nombre_categoria VARCHAR(50)   NOT NULL,
    fecha_carga      DATETIME      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_Dim_Producto PRIMARY KEY (ID_producto_sk)
);
GO

-- ------------------------------------------------------------
--  DIM_PEDIDO
--  Eliminados: comentarios  → texto libre, no filtrable ni agregable
-- ------------------------------------------------------------
CREATE TABLE dbo.Dim_Pedido (
    ID_pedido_sk        INT     IDENTITY(1,1) NOT NULL,
    ID_pedido_bk        INT     NOT NULL,
    estado              VARCHAR(15) NOT NULL,
    fecha_esperada      DATE        NOT NULL,
    fecha_entrega       DATE        NULL,
    -- Métricas de desempeño logístico calculadas y persistidas
    dias_retraso        AS (
                            CASE
                                WHEN fecha_entrega IS NOT NULL
                                     AND fecha_entrega > fecha_esperada
                                THEN DATEDIFF(DAY, fecha_esperada, fecha_entrega)
                                ELSE 0
                            END
                        ) PERSISTED,
    entregado_a_tiempo  AS (
                            CASE
                                WHEN fecha_entrega IS NULL       THEN CAST(0 AS BIT)
                                WHEN fecha_entrega <= fecha_esperada THEN CAST(1 AS BIT)
                                ELSE CAST(0 AS BIT)
                            END
                        ) PERSISTED,
    fecha_carga         DATETIME NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_Dim_Pedido PRIMARY KEY (ID_pedido_sk)
);
GO


-- ============================================================
--  TABLA DE HECHOS: FACT_Ventas
--  Granularidad: una línea de detalle de pedido
--  Todas las métricas numéricas se calculan y persisten aquí
-- ============================================================
CREATE TABLE dbo.FACT_Ventas (
    ID_hecho        BIGINT        IDENTITY(1,1) NOT NULL,
    -- Claves foráneas a dimensiones
    FK_tiempo       INT           NOT NULL,
    FK_cliente      INT           NOT NULL,
    FK_producto     INT           NOT NULL,
    FK_empleado     INT           NOT NULL,
    FK_oficina      INT           NOT NULL,
    FK_pedido       INT           NOT NULL,
    -- Business key para trazabilidad con el sistema origen
    ID_detalle_bk   INT           NOT NULL,
    -- Métricas base (almacenadas tal como vienen del origen)
    cantidad        INT           NOT NULL,
    precio_unidad   NUMERIC(15,2) NOT NULL,   -- Precio real de venta en ese momento
    costo_unitario  NUMERIC(15,2) NULL,       -- precio_proveedor en el momento de la venta
    -- Métricas calculadas y persistidas
    ingreso_total   AS (CAST(cantidad AS NUMERIC(15,2)) * precio_unidad) PERSISTED,
    costo_total     AS (
                        CASE
                            WHEN costo_unitario IS NOT NULL
                            THEN CAST(cantidad AS NUMERIC(15,2)) * costo_unitario
                            ELSE NULL
                        END
                    ) PERSISTED,
    margen_bruto    AS (
                        CASE
                            WHEN costo_unitario IS NOT NULL
                            THEN (CAST(cantidad AS NUMERIC(15,2)) * precio_unidad)
                               - (CAST(cantidad AS NUMERIC(15,2)) * costo_unitario)
                            ELSE NULL
                        END
                    ) PERSISTED,
    -- Metadata ETL
    fecha_carga     DATETIME      NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_FACT_Ventas   PRIMARY KEY (ID_hecho),
    CONSTRAINT FK_FV_Tiempo     FOREIGN KEY (FK_tiempo)   REFERENCES dbo.Dim_Tiempo   (ID_tiempo),
    CONSTRAINT FK_FV_Cliente    FOREIGN KEY (FK_cliente)  REFERENCES dbo.Dim_Cliente  (ID_cliente_sk),
    CONSTRAINT FK_FV_Producto   FOREIGN KEY (FK_producto) REFERENCES dbo.Dim_Producto (ID_producto_sk),
    CONSTRAINT FK_FV_Empleado   FOREIGN KEY (FK_empleado) REFERENCES dbo.Dim_Empleado (ID_empleado_sk),
    CONSTRAINT FK_FV_Oficina    FOREIGN KEY (FK_oficina)  REFERENCES dbo.Dim_Oficina  (ID_oficina_sk),
    CONSTRAINT FK_FV_Pedido     FOREIGN KEY (FK_pedido)   REFERENCES dbo.Dim_Pedido   (ID_pedido_sk)
);
GO

-- Índices en todas las FK para acelerar JOINs analíticos
CREATE NONCLUSTERED INDEX IX_FV_Tiempo    ON dbo.FACT_Ventas (FK_tiempo);
CREATE NONCLUSTERED INDEX IX_FV_Cliente   ON dbo.FACT_Ventas (FK_cliente);
CREATE NONCLUSTERED INDEX IX_FV_Producto  ON dbo.FACT_Ventas (FK_producto);
CREATE NONCLUSTERED INDEX IX_FV_Empleado  ON dbo.FACT_Ventas (FK_empleado);
CREATE NONCLUSTERED INDEX IX_FV_Oficina   ON dbo.FACT_Ventas (FK_oficina);
CREATE NONCLUSTERED INDEX IX_FV_Pedido    ON dbo.FACT_Ventas (FK_pedido);
GO


-- ============================================================
--  STORED PROCEDURE: Carga del calendario Dim_Tiempo
-- ============================================================
CREATE OR ALTER PROCEDURE dbo.sp_Cargar_Dim_Tiempo
    @FechaInicio DATE = '2020-01-01',
    @FechaFin    DATE = '2030-12-31'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Fecha DATE = @FechaInicio;

    WHILE @Fecha <= @FechaFin
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM dbo.Dim_Tiempo
            WHERE ID_tiempo = CONVERT(INT, FORMAT(@Fecha, 'yyyyMMdd'))
        )
        BEGIN
            INSERT INTO dbo.Dim_Tiempo (
                ID_tiempo, fecha, anio, trimestre, mes, nombre_mes,
                semana_anio, dia_mes, nombre_dia, es_fin_semana
            )
            VALUES (
                CONVERT(INT, FORMAT(@Fecha, 'yyyyMMdd')),
                @Fecha,
                YEAR(@Fecha),
                DATEPART(QUARTER,  @Fecha),
                MONTH(@Fecha),
                DATENAME(MONTH,    @Fecha),
                DATEPART(ISO_WEEK, @Fecha),
                DAY(@Fecha),
                DATENAME(WEEKDAY,  @Fecha),
                CASE WHEN DATEPART(WEEKDAY, @Fecha) IN (1,7) THEN 1 ELSE 0 END
            );
        END
        SET @Fecha = DATEADD(DAY, 1, @Fecha);
    END
END;
GO

EXEC dbo.sp_Cargar_Dim_Tiempo @FechaInicio = '2020-01-01', @FechaFin = '2030-12-31';
GO


-- ============================================================
--  VISTAS ANALÍTICAS
-- ============================================================

-- Detalle completo de cada línea de venta con todas las dimensiones
CREATE OR ALTER VIEW dbo.vw_Ventas_Detalle AS
SELECT
    -- Tiempo
    t.fecha,
    t.anio,
    t.nombre_trimestre,
    t.nombre_mes,
    t.es_fin_semana,
    -- Cliente
    c.nombre_cliente,
    c.ciudad          AS ciudad_cliente,
    c.region          AS region_cliente,
    c.pais            AS pais_cliente,
    c.limite_credito,
    -- Producto
    p.codigo_producto,
    p.nombre_producto,
    p.nombre_categoria,
    p.proveedor,
    -- Empleado / vendedor
    e.nombre_completo AS vendedor,
    e.puesto          AS puesto_vendedor,
    e.nombre_jefe,
    -- Oficina
    o.descripcion     AS oficina,
    o.ciudad          AS ciudad_oficina,
    o.pais            AS pais_oficina,
    o.region          AS region_oficina,
    -- Pedido
    pd.estado         AS estado_pedido,
    pd.dias_retraso,
    pd.entregado_a_tiempo,
    -- Métricas
    f.cantidad,
    f.precio_unidad,
    f.costo_unitario,
    f.ingreso_total,
    f.costo_total,
    f.margen_bruto
FROM       dbo.FACT_Ventas  f
JOIN       dbo.Dim_Tiempo   t  ON f.FK_tiempo   = t.ID_tiempo
JOIN       dbo.Dim_Cliente  c  ON f.FK_cliente  = c.ID_cliente_sk
JOIN       dbo.Dim_Producto p  ON f.FK_producto = p.ID_producto_sk
JOIN       dbo.Dim_Empleado e  ON f.FK_empleado = e.ID_empleado_sk
JOIN       dbo.Dim_Oficina  o  ON f.FK_oficina  = o.ID_oficina_sk
JOIN       dbo.Dim_Pedido   pd ON f.FK_pedido   = pd.ID_pedido_sk
WHERE      c.es_vigente = 1;   -- Solo versión vigente del cliente (SCD Tipo 2)
GO

-- Resumen de ventas agrupado por mes y categoría de producto
CREATE OR ALTER VIEW dbo.vw_Ventas_Mes_Categoria AS
SELECT
    t.anio,
    t.mes,
    t.nombre_mes,
    p.nombre_categoria,
    COUNT(*)             AS num_lineas,
    SUM(f.cantidad)      AS unidades_vendidas,
    SUM(f.ingreso_total) AS ingreso_total,
    SUM(f.margen_bruto)  AS margen_total,
    AVG(f.precio_unidad) AS precio_promedio
FROM       dbo.FACT_Ventas  f
JOIN       dbo.Dim_Tiempo   t ON f.FK_tiempo   = t.ID_tiempo
JOIN       dbo.Dim_Producto p ON f.FK_producto = p.ID_producto_sk
GROUP BY   t.anio, t.mes, t.nombre_mes, p.nombre_categoria;
GO


-- ============================================================
--  FIN DEL SCRIPT
-- ============================================================
PRINT '================================================';
PRINT 'DW_Ventas (optimizado) creado exitosamente.';
PRINT '------------------------------------------------';
PRINT 'Dimensiones : Dim_Tiempo, Dim_Oficina, Dim_Empleado,';
PRINT '              Dim_Cliente, Dim_Producto, Dim_Pedido';
PRINT 'Hechos      : FACT_Ventas';
PRINT 'Vistas      : vw_Ventas_Detalle, vw_Ventas_Mes_Categoria';
PRINT '================================================';
GO