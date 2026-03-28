--Crear la Base de Datos Staging
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'jardineria_staging')
    CREATE DATABASE jardineria_staging;

USE jardineria_staging;

--Tabla 1 de 5 — STG_EMPLEADO  (absorbe oficina)
IF OBJECT_ID('dbo.STG_EMPLEADO','U') IS NOT NULL DROP TABLE dbo.STG_EMPLEADO;

CREATE TABLE dbo.STG_EMPLEADO (
    STG_ID_EMPLEADO     INT              NOT NULL,
    STG_NOMBRE_COMPLETO NVARCHAR(150)    NOT NULL,   -- nombre + apellido1 + apellido2
    STG_PUESTO          NVARCHAR(50)     NULL,
    -- Campos desnormalizados de oficina:
    STG_OFI_CODIGO      NVARCHAR(10)     NULL,       -- ej. MAD-ES, BCN-ES
    STG_OFI_CIUDAD      NVARCHAR(30)     NULL,
    STG_OFI_PAIS        NVARCHAR(50)     NULL,
    STG_OFI_REGION      NVARCHAR(50)     NULL,       -- ej. EMEA, APAC
    -- Auditoría ETL:
    STG_FECHA_CARGA     DATETIME2        NOT NULL DEFAULT GETDATE(),
    STG_ORIGEN          NVARCHAR(50)     NOT NULL DEFAULT 'jardineria',
    CONSTRAINT PK_STG_EMPLEADO PRIMARY KEY (STG_ID_EMPLEADO)
);

-- Tabla 2 de 5 — STG_CLIENTE  (absorbe representante de ventas)
IF OBJECT_ID('dbo.STG_CLIENTE','U') IS NOT NULL DROP TABLE dbo.STG_CLIENTE;

CREATE TABLE dbo.STG_CLIENTE (
    STG_ID_CLIENTE      INT              NOT NULL,
    STG_NOMBRE_CLIENTE  NVARCHAR(50)     NOT NULL,
    STG_CIUDAD          NVARCHAR(50)     NULL,
    STG_REGION          NVARCHAR(50)     NULL,
    STG_PAIS            NVARCHAR(50)     NULL,
    STG_LIMITE_CREDITO  NUMERIC(15,2)    NULL,
    -- Campos desnormalizados del representante de ventas:
    STG_REP_NOMBRE      NVARCHAR(150)    NULL,       -- nombre completo del representante
    STG_REP_PUESTO      NVARCHAR(50)     NULL,
    STG_REP_OFI_CIUDAD  NVARCHAR(30)     NULL,       -- ciudad de la oficina del representante
    STG_REP_OFI_PAIS    NVARCHAR(50)     NULL,
    -- Auditoría ETL:
    STG_FECHA_CARGA     DATETIME2        NOT NULL DEFAULT GETDATE(),
    STG_ORIGEN          NVARCHAR(50)     NOT NULL DEFAULT 'jardineria',
    CONSTRAINT PK_STG_CLIENTE PRIMARY KEY (STG_ID_CLIENTE)
);

-- Tabla 3 de 5 — STG_PRODUCTO  (absorbe categoría)
IF OBJECT_ID('dbo.STG_PRODUCTO','U') IS NOT NULL DROP TABLE dbo.STG_PRODUCTO;

CREATE TABLE dbo.STG_PRODUCTO (
    STG_ID_PRODUCTO      INT              NOT NULL,
    STG_CODIGO_PRODUCTO  NVARCHAR(15)     NOT NULL,
    STG_NOMBRE           NVARCHAR(70)     NOT NULL,
    -- Campo desnormalizado de categoría:
    STG_CATEGORIA        NVARCHAR(50)     NULL,       -- Desc_Categoria de Categoria_producto
    STG_PROVEEDOR        NVARCHAR(50)     NULL,
    STG_CANTIDAD_STOCK   INT              NOT NULL DEFAULT 0,
    STG_PRECIO_VENTA     NUMERIC(15,2)    NOT NULL,
    STG_PRECIO_PROVEEDOR NUMERIC(15,2)    NULL,
    -- Columna calculada persistida:
    STG_MARGEN_BRUTO     AS (STG_PRECIO_VENTA
                          - ISNULL(STG_PRECIO_PROVEEDOR, 0)) PERSISTED,
    -- Auditoría ETL:
    STG_FECHA_CARGA      DATETIME2        NOT NULL DEFAULT GETDATE(),
    STG_ORIGEN           NVARCHAR(50)     NOT NULL DEFAULT 'jardineria',
    CONSTRAINT PK_STG_PRODUCTO  PRIMARY KEY (STG_ID_PRODUCTO),
    CONSTRAINT UQ_STG_COD_PROD  UNIQUE (STG_CODIGO_PRODUCTO)
);

-- Tabla 4 de 5 — STG_PEDIDO  (absorbe datos geográficos del cliente)
IF OBJECT_ID('dbo.STG_PEDIDO','U') IS NOT NULL DROP TABLE dbo.STG_PEDIDO;

CREATE TABLE dbo.STG_PEDIDO (
    STG_ID_PEDIDO       INT              NOT NULL,
    STG_FECHA_PEDIDO    DATE             NOT NULL,
    STG_FECHA_ESPERADA  DATE             NOT NULL,
    STG_FECHA_ENTREGA   DATE             NULL,
    STG_ESTADO          NVARCHAR(20)     NOT NULL,
    STG_ID_CLIENTE      INT              NOT NULL,
    -- Campos desnormalizados del cliente:
    STG_CLI_NOMBRE      NVARCHAR(50)     NULL,       -- nombre del cliente
    STG_CLI_CIUDAD      NVARCHAR(50)     NULL,       -- ciudad del cliente
    STG_CLI_PAIS        NVARCHAR(50)     NULL,       -- país del cliente
    -- Columnas calculadas persistidas:
    STG_ANIO_PEDIDO     AS (YEAR(STG_FECHA_PEDIDO))  PERSISTED,
    STG_MES_PEDIDO      AS (MONTH(STG_FECHA_PEDIDO)) PERSISTED,
    -- Columna calculada no persistida (depende de fecha_entrega nullable):
    STG_DIAS_RETRASO    AS (CASE WHEN STG_FECHA_ENTREGA IS NOT NULL
                               THEN DATEDIFF(DAY, STG_FECHA_ESPERADA, STG_FECHA_ENTREGA)
                               ELSE NULL END),
    -- Auditoría ETL:
    STG_FECHA_CARGA     DATETIME2        NOT NULL DEFAULT GETDATE(),
    STG_ORIGEN          NVARCHAR(50)     NOT NULL DEFAULT 'jardineria',
    CONSTRAINT PK_STG_PEDIDO  PRIMARY KEY (STG_ID_PEDIDO),
    CONSTRAINT FK_STG_PED_CLI FOREIGN KEY (STG_ID_CLIENTE)
        REFERENCES dbo.STG_CLIENTE (STG_ID_CLIENTE)
);

-- Tabla 5 de 5 — STG_DETALLE_PEDIDO  (tabla de hechos, absorbe nombre y categoría de producto)
IF OBJECT_ID('dbo.STG_DETALLE_PEDIDO','U') IS NOT NULL DROP TABLE dbo.STG_DETALLE_PEDIDO;

CREATE TABLE dbo.STG_DETALLE_PEDIDO (
    STG_ID_DETALLE      INT              IDENTITY(1,1) NOT NULL,
    STG_ID_PEDIDO       INT              NOT NULL,
    STG_ID_PRODUCTO     INT              NOT NULL,
    STG_CANTIDAD        INT              NOT NULL,
    STG_PRECIO_UNIDAD   NUMERIC(15,2)    NOT NULL,
    -- Campos desnormalizados del producto:
    STG_PRO_NOMBRE      NVARCHAR(70)     NULL,       -- nombre del producto
    STG_PRO_CATEGORIA   NVARCHAR(50)     NULL,       -- categoría del producto
    -- Columna calculada persistida:
    STG_IMPORTE_LINEA   AS (STG_CANTIDAD * STG_PRECIO_UNIDAD) PERSISTED,
    -- Auditoría ETL:
    STG_FECHA_CARGA     DATETIME2        NOT NULL DEFAULT GETDATE(),
    STG_ORIGEN          NVARCHAR(50)     NOT NULL DEFAULT 'jardineria',
    CONSTRAINT PK_STG_DETALLE   PRIMARY KEY (STG_ID_DETALLE),
    CONSTRAINT FK_STG_DET_PED   FOREIGN KEY (STG_ID_PEDIDO)
        REFERENCES dbo.STG_PEDIDO (STG_ID_PEDIDO),
    CONSTRAINT FK_STG_DET_PRO   FOREIGN KEY (STG_ID_PRODUCTO)
        REFERENCES dbo.STG_PRODUCTO (STG_ID_PRODUCTO)
);

-- Tabla STG_PAGO  (absorbe nombre del cliente)
IF OBJECT_ID('dbo.STG_PAGO','U') IS NOT NULL DROP TABLE dbo.STG_PAGO;

CREATE TABLE dbo.STG_PAGO (
    STG_ID_PAGO         INT              IDENTITY(1,1) NOT NULL,
    STG_ID_CLIENTE      INT              NOT NULL,
    STG_CLI_NOMBRE      NVARCHAR(50)     NULL,       -- nombre desnormalizado del cliente
    STG_FORMA_PAGO      NVARCHAR(40)     NOT NULL,
    STG_ID_TRANSACCION  NVARCHAR(50)     NOT NULL,
    STG_FECHA_PAGO      DATE             NOT NULL,
    STG_TOTAL           NUMERIC(15,2)    NOT NULL,
    -- Columnas calculadas persistidas:
    STG_ANIO_PAGO       AS (YEAR(STG_FECHA_PAGO))  PERSISTED,
    STG_MES_PAGO        AS (MONTH(STG_FECHA_PAGO)) PERSISTED,
    -- Auditoría ETL:
    STG_FECHA_CARGA     DATETIME2        NOT NULL DEFAULT GETDATE(),
    STG_ORIGEN          NVARCHAR(50)     NOT NULL DEFAULT 'jardineria',
    CONSTRAINT PK_STG_PAGO        PRIMARY KEY (STG_ID_PAGO),
    CONSTRAINT UQ_STG_TRANSACCION UNIQUE (STG_ID_TRANSACCION),
    CONSTRAINT FK_STG_PAG_CLI     FOREIGN KEY (STG_ID_CLIENTE)
        REFERENCES dbo.STG_CLIENTE (STG_ID_CLIENTE)
);

PRINT '>>> Fase 2 completada: 5 tablas STG creadas exitosamente.';

