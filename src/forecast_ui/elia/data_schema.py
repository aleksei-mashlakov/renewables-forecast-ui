import enum

import polars as pl

EliaHistorySolarSchema = pl.Schema([
    ("datetime", pl.String),
    ("measured", pl.Float64),
    ("dayahead11hforecast", pl.Float64),
    ("dayahead11hconfidence10", pl.Float64),
    ("dayahead11hconfidence90", pl.Float64),
    ("monitoredcapacity", pl.Float64),
    ("loadfactor", pl.Float64),
])

EliaRealTimeSolarSchema = pl.Schema([
    ("datetime", pl.String),
    ("realtime", pl.Float64),
    ("dayahead11hforecast", pl.Float64),
    ("dayahead11hconfidence10", pl.Float64),
    ("dayahead11hconfidence90", pl.Float64),
    ("monitoredcapacity", pl.Float64),
    ("loadfactor", pl.Float64),
])

EliaHistoryWindSchema = pl.Schema([
    ("datetime", pl.String),
    ("measured", pl.Float64),
    ("dayahead11hforecast", pl.Float64),
    ("dayahead11hconfidence10", pl.Float64),
    ("dayahead11hconfidence90", pl.Float64),
    ("monitoredcapacity", pl.Float64),
    ("loadfactor", pl.Float64),
    ("decrementalbidid", pl.String),
])


EliaRealTimeWindSchema = pl.Schema([
    ("datetime", pl.String),
    ("realtime", pl.Float64),
    ("dayahead11hforecast", pl.Float64),
    ("dayahead11hconfidence10", pl.Float64),
    ("dayahead11hconfidence90", pl.Float64),
    ("monitoredcapacity", pl.Float64),
    ("loadfactor", pl.Float64),
    ("decrementalbidid", pl.String),
])


class EliaDataset(enum.Enum):
    """Enum for Elia datasets."""

    WIND_REALTIME = ("ods086", EliaRealTimeWindSchema)
    SOLAR_REALTIME = ("ods087", EliaRealTimeSolarSchema)
    WIND_HISTORY = ("ods031", EliaHistoryWindSchema)
    SOLAR_HISTORY = ("ods032", EliaHistorySolarSchema)
