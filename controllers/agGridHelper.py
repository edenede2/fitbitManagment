from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import polars as pl
import pandas as pd

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
def build_grid_options(df_pl: pl.DataFrame,bool_editable: False) -> dict:
    gd = GridOptionsBuilder.from_dataframe(df_pl.to_pandas())
    gd.configure_default_column(
        filterable=True,
        sortable=True,
        resizable=True,
        floatingFilter=True,
    )

    for col, dtype in df_pl.schema.items():

        # --- Boolean columns: checkbox render & edit ----------------
        if dtype == pl.Boolean:
            common = dict(
                filter="agSetColumnFilter",
                cellRenderer="agCheckboxCellRenderer",  # always show tick
                width=120,
            )
            if bool_editable:
                # editable checkbox
                gd.configure_column(
                    col,
                    editable=True,
                    cellEditor="agCheckboxCellEditor",
                    **common
                )
            else:
                # read‑only checkbox
                gd.configure_column(col, editable=False, **common)
        # --- everything else: picked by dtype -----------------------
        else:
            gd.configure_column(col, filter=polars_dtype_to_ag_filter(dtype))

    return gd.build()

# ------------------------------------------------------------------ #
# 3.  put it together inside Streamlit
# ------------------------------------------------------------------ #
def aggrid_polars(df_pl: pl.DataFrame, bool_editable: bool = False) -> tuple:
    grid_response = AgGrid(
        df_pl.to_pandas(),
        gridOptions=build_grid_options(df_pl, bool_editable),
        theme="streamlit",                 # or "balham", "material", …
        update_mode=GridUpdateMode.MODEL_CHANGED,  # send back on every edit
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,          # ← required for checkbox renderer
    )

    # edited data back to Polars
    edited_df_pl = pl.from_pandas(pd.DataFrame(grid_response["data"]))
    return edited_df_pl, grid_response