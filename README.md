# pydantic-snowflake
[WORK IN PROGRESS]

Python package to read and write Pydantic V2 classes to Snowflake.

This package aims to ease the integration between Pydantic V2 and Snowflake. The idea is that if you already have your Pydantic class definitions, you can leverage them to write to Snowflake - instead of inferring the types via dictionaries or Pandas.

## From Pydantic V2 to Snowflake

What you would need is to have:
- a Pydantic V2 defition of you class. You could have defined it as a data contract with another party or system.
- An active connection to Snowflake. For example, via the [snowflake-connector](https://github.com/snowflakedb/snowflake-connector-python). You manage this connection (ie: you open/close it).
- Pandas installed: leveraged for writing. Ideally you also installed [snowflake-connector](https://github.com/snowflakedb/snowflake-connector-python) with `write_pandas` for faster insert.

With that, you can just:
```python
from pydantic_to_snowflake import PydanticToSnowflake
from pydantic import BaseModel, Field

# Dummy planetary data
class Planet(BaseModel):
    name: str
    km_to_Earth: int = Field(..., description="Ballpark ignoring orbits.")

planet_data = [
    {"name": "Mercury", "km_to_Earth": 91700000},
    {"name": "Venus", "km_to_Earth": 41400000},
    {"name": "Mars", "km_to_Earth": 78340000},
    {"name": "Jupiter", "km_to_Earth": 628730000},
    {"name": "Saturn", "km_to_Earth": 1275000000}
]
planets = [Planet(**data) for data in planet_data]

# Connect to Snowflake
import snowflake.connector as sc
database = ''
schema = ''
table_name = ''
conn_params = {...}
connection = sc.connect(**conn_params)
# instantiate
flakes = PydanticToSnowflake(connection=connection, 
                             database=database,
                             schema=schema, 
                             table=table_name)
# Write. Mind it will automatically uppercase the column names.
flakes.create_table_and_insert(model_class=Planet, data=planets)

# Close if you're done
connection.close()
```

### Considerations
Still WIP. 

Lists, dictionaries, etc are parsed to JSON via `PydanticToSnowflake._make_json_serializable()`. If that does not work with your requirements, there is a `custom_type_mapping` (dict) in the constructor.


### Contact
carlos.aguilar.palacios@gmail.com
