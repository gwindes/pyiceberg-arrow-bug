import os.path

from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa
import pandas as pd


def sanitize_ch_names(ch_name: str) -> str:
    """ Helper func to sanitize the column/channel names """
    chars_to_replace = [":", ".", "-", "/"]
    sanitized = ch_name
    for char in chars_to_replace:
        sanitized = sanitized.replace(char, "_")
    sanitized = sanitized.lower()
    return sanitized


""" 
Simple logic to create dataframe and save it to iceberg table.
Showcases issues with column name in pyarrow unless sanitized
"""

# Verify warehouse folder exists
if not os.path.exists("warehouse"):
    os.mkdir("warehouse")

data = {
    'TEST:A1B2.RAW.ABC-GG-1-A': [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
    'TEST:A1B2.RAW.ABC-GG-1-B': [0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9],
    'TEST:A1B2.RAW.ABC-GG-1-C': [0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9],
    'time': [
        1702090722998897808,
        1702090722998947809,
        1702090722998997809,
        1702090722999047809,
        1702090722999097809,
        1702090722999147809,
        1702090722999197809,
        1702090722999247809,
        1702090722999297809,
        1702090722999347809
    ]
}

df = pd.DataFrame(data)
pa_data = pa.Table.from_pandas(df)

"""
Uncomment to sanitize the channel names and make it work.
Delete the contents in warehouse folder and rerun.
"""
# ch_name_swap = dict()
# for ch_name in pa_data.column_names:
#     ch_name_swap[ch_name] = sanitize_ch_names(ch_name)
# pa_data = pa_data.rename_columns(ch_name_swap.values())

# iceberg warehouse local sqlite
warehouse_path = "./warehouse"
catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/pyiceberg-catalog.db",
        "warehouse": f"file://{warehouse_path}"
    }
)

# iceberg table creation/insertion
ns = catalog.list_namespaces()
if ("A1B2",) not in ns:
    catalog.create_namespace("A1B2")

tables = catalog.list_tables("A1B2")
if len(tables) == 0 or "A1-301" not in tables[0]:
    table = catalog.create_table("A1B2.A1-301", schema=pa_data.schema)
    table.append(pa_data)
    print("Added data")
else:
    table = catalog.load_table("A1B2.A1-301")

# This causes the error pyarrow.lib.ArrowInvalid:
# No match for FieldRef.Name(A1B2_x2ERAW_x2EABC_x2DGG_x2D1_x2DA) in A1B2.RAW.ABC-GG-1-A: double
df_pandas = table.scan().to_pandas()
df_pyarrow = table.scan().to_arrow()


print(df_pandas)
print(df_pyarrow)

