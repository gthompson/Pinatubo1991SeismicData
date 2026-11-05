"""Compatibility imports; the implementation is consolidated in step_50_build_obspy_catalog."""
from .step_50_build_obspy_catalog import (
    run, main, parse_seed_id, safe_series_get, build_pick, build_origin,
    associate_one_to_one_by_time, associate_nearest_by_time,
)
