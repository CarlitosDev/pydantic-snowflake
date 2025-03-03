import json
from typing import Type, List, Any, Optional, Dict, get_origin
from datetime import datetime, date
from pydantic import BaseModel

class PydanticToSnowflake:
    def __init__(self, connection: Any, database: str, schema: str, table: str, custom_type_mapping: Optional[Dict[Any, str]] = None):
        """
        Initialize with a connection (SQLAlchemy or native Snowflake connector), target schema, table,
        and an optional custom type mapping dictionary.
        """
        self.connection = connection
        self.database = database
        self.schema = schema
        self.table = table
        self.custom_type_mapping = custom_type_mapping or {}

    @staticmethod
    def get_snowflake_type(py_type: Any, custom_mapping: Optional[Dict[Any, str]] = None) -> str:
        """
        Maps a Python type (from a Pydantic field annotation) to a Snowflake SQL type.
        Checks for custom overrides first.
        """
        if custom_mapping:
            for key, sql_type in custom_mapping.items():
                try:
                    if isinstance(py_type, type) and issubclass(py_type, key):
                        return sql_type
                except TypeError:
                    continue
        origin = get_origin(py_type)
        if origin in (list, dict, tuple, set):
            return "VARIANT"
        try:
            if isinstance(py_type, type):
                if issubclass(py_type, bool):
                    return "BOOLEAN"
                if issubclass(py_type, int):
                    return "NUMBER"
                if issubclass(py_type, float):
                    return "FLOAT"
                if issubclass(py_type, str):
                    return "VARCHAR"
                if issubclass(py_type, datetime):
                    return "TIMESTAMP_NTZ"
                if issubclass(py_type, date):
                    return "DATE"
                if issubclass(py_type, BaseModel):
                    return "VARIANT"
        except Exception:
            pass
        return "VARIANT"

    @staticmethod
    def convert_value(val: Any) -> Any:
        """
        Converts complex values (nested Pydantic models, lists, dicts) to JSON strings.
        Useful for inserting into Snowflake's VARIANT columns.
        """
        if isinstance(val, BaseModel):
            return val.model_dump_json()
        elif isinstance(val, tuple):
            return json.dumps({str(key): value for key, value in val.items()})
        elif isinstance(val, (list, dict, set)):
            safe_val = PydanticToSnowflake._make_json_serializable(val)
            return json.dumps(safe_val)
        return val
    
    @staticmethod
    def _make_json_serializable(val: Any) -> Any:
        """
        Recursively converts values to a JSON-serializable format.
        - For dicts: converts non-string keys (like tuples) to strings.
        - For tuples: converts them to lists.
        - For BaseModel instances: returns their dict representation.
        - For NumPy scalar types: converts them to native Python types.
        """
        try:
            import numpy as np
            if isinstance(val, np.generic):
                return val.item()
        except ImportError:
            pass

        if isinstance(val, dict):
            new_dict = {}
            for k, v in val.items():
                # Convert non-string keys (e.g. tuples) to strings.
                new_key = k if isinstance(k, str) else str(k)
                new_dict[new_key] = PydanticToSnowflake._make_json_serializable(v)
            return new_dict
        elif isinstance(val, list):
            return [PydanticToSnowflake._make_json_serializable(item) for item in val]
        elif isinstance(val, tuple):
            # Convert tuples to lists.
            return [PydanticToSnowflake._make_json_serializable(item) for item in val]
        elif isinstance(val, BaseModel):
            return val.model_dump()
        return val

    @classmethod
    def to_dataframe(cls, model_class: Type[BaseModel], data: List[BaseModel], cols_to_uppecase: bool = False) -> Any:
        """
        Class method to convert a list of Pydantic model instances into a Pandas DataFrame.
        This conversion ensures that any complex types are properly converted for insertion into Snowflake.
        """
        import pandas as pd
        col_names = list(model_class.model_fields.keys())
        df = pd.DataFrame([
            {col: cls.convert_value(getattr(item, col)) for col in col_names}
            for item in data
        ])
        if cols_to_uppecase:
            df.columns = df.columns.str.upper()
        return df

    def create_table(self, model_class: Type[BaseModel]) -> None:
        """
        Creates or replaces the target table in Snowflake based on the Pydantic model's schema.
        """
        import snowflake.connector.connection as sc
        create_table_sql = self.get_create_table_sql(model_class)
        try:
            cur = self.connection.cursor()
            cur.execute(create_table_sql)
        except Exception as e:
            try:
                self.connection.execute(create_table_sql)
            except Exception as e2:
                raise RuntimeError(f"Table creation failed: {e}; fallback also failed: {e2}")
            
    def get_create_table_sql(self, model_class: Type[BaseModel]) -> str:
        columns = []
        for field_name, field_info in model_class.model_fields.items():
            sql_type = self.get_snowflake_type(field_info.annotation, custom_mapping=self.custom_type_mapping)
            columns.append(f'{field_name.upper()} {sql_type}')
            # columns.append(f'"{field_name.upper()}" {sql_type}')
        columns_def = ", ".join(columns)
        create_table_stmt = f"CREATE OR REPLACE TABLE {self.database}.{self.schema}.{self.table} ({columns_def})"
        return create_table_stmt        
        

    def check_table_schema(self, model_class: Type[BaseModel]) -> bool:
        """
        Checks if the table exists and if it contains the expected columns and data types
        based on the provided Pydantic model.
        Returns True if the table exists and the schema matches, otherwise False.
        """
        try:
            cur = self.connection.cursor()
            query = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{self.schema.upper()}' 
              AND TABLE_NAME = '{self.table.upper()}'
            ORDER BY ORDINAL_POSITION;
            """
            cur.execute(query)
            rows = cur.fetchall()
            cur.close()
            if not rows:
                return False

            expected = []
            for field_name, field_info in model_class.model_fields.items():
                expected_type = self.get_snowflake_type(field_info.annotation, custom_mapping=self.custom_type_mapping).upper()
                expected.append((field_name.upper(), expected_type))
            if len(expected) != len(rows):
                return False
            for (exp_col, exp_type), (act_col, act_type, *_) in zip(expected, rows):
                if exp_col != act_col.upper():
                    return False
                if exp_type not in act_type.upper():
                    return False
            return True
        except Exception:
            return False

    def insert_data(self, model_class: Type[BaseModel], data: List[BaseModel]) -> None:
        """
        Inserts the data into the target table. Tries to use the optimal method based on the connection type:
          - If using SQLAlchemy, it uses Pandas' to_sql with method "multi".
          - If using a native Snowflake connector, it attempts to use write_pandas.
          - Otherwise, it falls back to executemany.
        """
        import pandas as pd
        df = self.to_dataframe(model_class, data, cols_to_uppecase=True)
        if hasattr(self.connection, "dialect"):
            try:
                df.to_sql(name=self.table, con=self.connection, schema=self.schema,
                          if_exists="append", index=False, method="multi")
            except Exception as e:
                raise RuntimeError(f"SQLAlchemy insertion failed: {e}")
        else:
            try:
                from snowflake.connector.pandas_tools import write_pandas
                success, nchunks, nrows, _ = write_pandas(self.connection, df, table_name=self.table, database=self.database, schema=self.schema, use_logical_type=True)
                if not success:
                    raise RuntimeError("write_pandas failed to insert data.")
            except ImportError:
                col_names = list(model_class.model_fields.keys())
                placeholders = ", ".join(["%s"] * len(col_names))
                insert_stmt = f"INSERT INTO {self.schema}.{self.table} ({', '.join([f'{col.upper()}' for col in col_names])}) VALUES ({placeholders})"
                records = [tuple(self.convert_value(getattr(item, col)) for col in col_names) for item in data]
                cur = self.connection.cursor()
                cur.executemany(insert_stmt, records)
                self.connection.commit()
                cur.close()
            except Exception as e:
                try:
                    col_names = list(model_class.model_fields.keys())
                    placeholders = ", ".join(["%s"] * len(col_names))
                    insert_stmt = f"INSERT INTO {self.schema}.{self.table} ({', '.join([f'\"{col.upper()}\"' for col in col_names])}) VALUES ({placeholders})"
                    records = [tuple(self.convert_value(getattr(item, col)) for col in col_names) for item in data]
                    cur = self.connection.cursor()
                    cur.executemany(insert_stmt, records)
                    self.connection.commit()
                    cur.close()
                except Exception as e2:
                    raise RuntimeError(f"Data insertion failed: {e}; fallback also failed: {e2}")

    def create_table_and_insert(self, model_class: Type[BaseModel], data: List[BaseModel], skip_table_creation_if_valid: bool = False) -> None:
        """
        Convenience method that optionally checks the table schema and creates (or replaces) the target table 
        before inserting the provided data.
        
        Parameters:
          - model_class: The Pydantic model class describing the table schema.
          - data: A list of Pydantic model instances to insert.
          - skip_table_creation_if_valid: If True, the method will only insert records if check_table_schema returns True;
            otherwise, it creates (or replaces) the table before insertion.
        """
        if skip_table_creation_if_valid:
            if not self.check_table_schema(model_class):
                self.create_table(model_class)
        else:
            self.create_table(model_class)
        self.insert_data(model_class, data)