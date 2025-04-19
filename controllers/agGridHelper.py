from st_aggrid import AgGrid, GridUpdateMode, DataReturnMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import polars as pl
import pandas as pd
import streamlit as st
# ------------------------------------------------------------------ #
# 1.  dtype  ➜  AG‑Grid filter
# ------------------------------------------------------------------ #
def ag_filter(dtype: pl.DataType) -> str:
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

def build_grid_options(df_pl: pl.DataFrame, *, bool_editable: bool, selection_mode="multiple") -> dict:
    gd = GridOptionsBuilder.from_dataframe(df_pl.to_pandas())
    gd.configure_default_column(filterable=True, sortable=True,
                                resizable=True, floatingFilter=True)
    
    # Add row selection capability
    gd.configure_selection(selection_mode=selection_mode, use_checkbox=True)
    
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
                    cellEditorParams={
                        'useFormatter': True,
                    },
                    valueFormatter="data ? 'True' : 'False'",
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
                  *, bool_editable: bool = False, key: str = None,
                  selection_mode="multiple", pre_selected_rows=None):
    """Show a Polars DF in Ag‑Grid and return edited DF + full response."""
    
    # Create grid options with selection mode
    grid_options = build_grid_options(df_pl, bool_editable=bool_editable, selection_mode=selection_mode)
    
    # Convert to pandas for AgGrid
    df_pd = df_pl.to_pandas()
    
    # Create a container to maintain state across rerenders
    if f"aggrid_state_{key}" not in st.session_state:
        st.session_state[f"aggrid_state_{key}"] = {
            "selected_rows": pre_selected_rows if pre_selected_rows else []
        }
    
    # resp = AgGrid(
    #     df_pd,
    #     key=key,
    #     gridOptions=grid_options,
    #     theme="streamlit",
    #     fit_columns_on_grid_load=True,
    #     allow_unsafe_jscode=True,
    #     update_mode=GridUpdateMode.SELECTION_CHANGED,  # Changed from VALUE_CHANGED to MODEL_CHANGED
    #     data_return_mode=DataReturnMode.FILTERED_AND_SORTED,  # Changed to get more complete data
    #     pre_selected_rows=st.session_state[f"aggrid_state_{key}"]["selected_rows"],
    # )
    resp = AgGrid(
        df_pd,
        key=key,
        gridOptions=grid_options,
        theme="fresh",
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,
        update_mode=GridUpdateMode.SELECTION_CHANGED,  # Changed from VALUE_CHANGED to MODEL_CHANGED
        height=500,
        # data_return_mode=DataReturnMode.FILTERED_AND_SORTED,  # Changed to get more complete data
        # pre_selected_rows=st.session_state[f"aggrid_state_{key}"]["selected_rows"],
    )
    st.write(resp['selected_rows'])
    
    # Save the selected rows for next render
    if resp.get("selected_rows"):
        st.session_state[f"aggrid_state_{key}"]["selected_rows"] = resp["selected_rows"]
        st.write(st.session_state[f"aggrid_state_{key}"]["selected_rows"])
    # Convert the returned data back to polars
    edited_df = pl.from_pandas(pd.DataFrame(resp["data"]))

    return edited_df, resp