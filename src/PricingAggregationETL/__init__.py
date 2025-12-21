"""
PricingAggregationETL - Pricing data aggregation ETL pipeline for data warehouse analytics
"""

from .PricingAggregationETL import (
    PricingAggregationETL,
    PreValidationPhase
)

__version__ = "0.1.0"

__all__ = [
    "PricingAggregationETL",
    "PreValidationPhase"
]
