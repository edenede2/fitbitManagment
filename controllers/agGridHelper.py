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

def build_grid_options(df_pl: pl.DataFrame, *, bool_editable: bool) -> dict:
    gd = GridOptionsBuilder.from_dataframe(df_pl.to_pandas())
    gd.configure_default_column(filterable=True, sortable=True,
                                resizable=True, floatingFilter=True)

    for col, dtype in df_pl.schema.items():
        if dtype == pl.Boolean:
            common = dict(
                filter="agSetColumnFilter",
                cellRenderer="agCheckboxCellRenderer",  # ⇠ shows a tick
                width=110,
            )
            if bool_editable:
                gd.configure_column(
                    col,
                    editable=True,
                    cellEditor="agCheckboxCellEditor",   # ⇠ commits value
                    **common,
                )
            else:
                # read‑only tick (greyed‑out)
                gd.configure_column(
                    col,
                    editable=False,
                    cellRendererParams={"disabled": True},
                    **common,
                )
        else:
            gd.configure_column(col, filter=ag_filter(dtype))

    return gd.build()

def aggrid_polars(df_pl: pl.DataFrame,
                  *, bool_editable: bool = False, key: str = None):
    """Show a Polars DF in Ag‑Grid and return edited DF + full response."""
    resp = AgGrid(
        df_pl.to_pandas(),
        key=key,
        gridOptions=build_grid_options(df_pl, bool_editable=bool_editable),
        theme="streamlit",
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,
        update_mode=GridUpdateMode.VALUE_CHANGED,
        data_return_mode=DataReturnMode.AS_INPUT,
    )
    edited_df = pl.from_pandas(pd.DataFrame(resp["data"]))
    return edited_df, resp