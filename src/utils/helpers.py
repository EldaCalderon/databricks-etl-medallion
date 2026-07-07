from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from datetime import datetime


def configure_managed_identity(spark, storage_account: str) -> None:
    """Configura autenticación por Managed Identity hacia ADLS Gen2."""
    base = f"fs.azure.account"
    account_url = f"{storage_account}.dfs.core.windows.net"
    spark.conf.set(f"{base}.auth.type.{account_url}", "OAuth")
    spark.conf.set(
        f"{base}.oauth.provider.type.{account_url}",
        "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
    )
    print(f"[OK] Managed Identity configurado para: {account_url}")


def add_ingestion_metadata(df: DataFrame, source_name: str) -> DataFrame:
    """Agrega columnas de auditoría estándar a cualquier DataFrame."""
    return (
        df
        .withColumn("_source",         F.lit(source_name))
        .withColumn("_ingestion_date", F.current_timestamp())
        .withColumn("_year",           F.year(F.current_timestamp()))
        .withColumn("_month",          F.month(F.current_timestamp()))
    )


def log_counts(df: DataFrame, layer: str, table: str) -> None:
    """Imprime conteo de registros para auditoría de pipeline."""
    count = df.count()
    print(f"[{layer.upper()}] {table}: {count:,} registros")


def drop_full_duplicates(df: DataFrame) -> DataFrame:
    """Elimina filas completamente duplicadas."""
    before = df.count()
    df_clean = df.dropDuplicates()
    after = df_clean.count()
    print(f"[DEDUP] Eliminados {before - after:,} duplicados")
    return df_clean


def cast_column_safe(df: DataFrame, col_name: str, target_type) -> DataFrame:
    """Castea una columna al tipo indicado; retorna null si falla."""
    return df.withColumn(col_name, F.col(col_name).cast(target_type))


def fill_nulls_with_unknown(df: DataFrame, columns: list) -> DataFrame:
    """Rellena nulos de columnas de texto con 'Unknown'."""
    fill_map = {col: "Unknown" for col in columns}
    return df.fillna(fill_map)


def write_delta(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """Escribe un DataFrame como tabla Delta con logging."""
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"[WRITE] Tabla '{table_name}' escrita en modo '{mode}'")


def create_schema_if_not_exists(spark, catalog: str, schema: str) -> None:
    """Crea el schema (base de datos) si no existe en Unity Catalog."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"[SCHEMA] {catalog}.{schema} verificado/creado")
