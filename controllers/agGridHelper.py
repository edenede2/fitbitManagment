from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
import polars as pl


# --- helper ---------------------------------------------------------------
def polars_dtype_to_ag_filter(dtype: pl.datatypes.DataType) -> str:
    """Return AG‑Grid filter name for a Polars dtype."""
    if dtype.is_numeric():
        return "agNumberColumnFilter"
    if dtype == pl.Boolean:
        return "agSetColumnFilter"       # nice checkbox filter
    if dtype in (pl.Datetime, pl.Date):
        return "agDateColumnFilter"      # ⇢ needs ISO dates or comparator!
    return "agTextColumnFilter"

def configure_filters_from_polars(gd: GridOptionsBuilder, df_pl: pl.DataFrame) -> None:
    """Configure each AG‑Grid column filter according to Polars dtypes."""
    for col, dtype in df_pl.schema.items():
        gd.configure_column(col, filter=polars_dtype_to_ag_filter(dtype))