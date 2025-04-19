from st_aggrid import AgGrid, GridUpdateMode, DataReturnMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import polars as pl
import pandas as pd
import streamlit as st
# ------------------------------------------------------------------ #
# 1.  dtype  ➜  AG‑Grid filter
# ------------------------------------------------------------------ #
def polars_dtype_to_ag_filter(dtype: pl.DataType) -> str:
    if dtype.is_numeric():
        return "agNumberColumnFilter"
    if dtype == pl.Boolean:
        return "agSetColumnFilter"         # ✓ / ✗ set filter
    if dtype in (pl.Datetime, pl.Date):
        return "agDateColumnFilter"
    return "agTextColumnFilter"

# ------------------------------------------------------------------ #
# 2.  build GridOptionsBuilder automatically from Polars schema
# ------------------------------------------------------------------ #

def build_grid_options(df_pl: pl.DataFrame,
                       bool_editable: bool = False) -> dict:
    gd = GridOptionsBuilder.from_dataframe(df_pl.to_pandas())
    gd.configure_default_column(
        filterable=True,
        sortable=True,
        resizable=True,
        floatingFilter=True,
    )

    for col, dtype in df_pl.schema.items():
        if dtype == pl.Boolean:
            common = dict(
                filter="agSetColumnFilter",
                cellRenderer="booleanCellRenderer",   # ✓ / ✗
                width=110,
            )
            if bool_editable:
                gd.configure_column(
                    col,
                    editable=True,
                    cellEditor="agSelectCellEditor",
                    cellEditorParams={"values": [True, False]},
                    **common
                )
            else:
                gd.configure_column(col, editable=False, **common)
        else:
            gd.configure_column(col, filter=polars_dtype_to_ag_filter(dtype))

    return gd.build()

# ------------------------------------------------------------------ #
# 3.  put it together inside Streamlit
# ------------------------------------------------------------------ #
def aggrid_polars(df_pl: pl.DataFrame, bool_editable: bool = False, key: str = 'key') -> tuple:
    grid_response = AgGrid(
        df_pl.to_pandas(),
        gridOptions=build_grid_options(df_pl, bool_editable),
        theme="streamlit",
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,              # need for builtin renderers
        update_mode=GridUpdateMode.VALUE_CHANGED,  # fire on every cell edit
        data_return_mode=DataReturnMode.AS_INPUT,  # return *all* rows
        key=key,                              # ← now forwarded

    )
    # convert back to Polars
    edited = pl.from_pandas(pd.DataFrame(grid_response["data"]))
    return edited, grid_response