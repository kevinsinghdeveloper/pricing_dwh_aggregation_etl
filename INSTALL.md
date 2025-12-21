# Installation Guide

## Quick Start

### Install from Local Repository

```bash
# Navigate to the repository directory
cd /path/to/PricingAggregationETL

# Install in editable mode (recommended for development)
pip install -e .
```

### Install from Git

```bash
pip install git+https://github.com/zerveme/PricingAggregationETL.git
```

## Verify Installation

After installation, you can verify that the package is correctly installed:

```python
from PricingAggregationETL import PricingAggregationETL
print("PricingAggregationETL imported successfully!")
```

## Package Structure

```
pricing_dwh_aggregation_etl/
├── src/
│   └── PricingAggregationETL/
│       ├── __init__.py
│       └── PricingAggregationETL.py
├── test/                # Test files
├── pyproject.toml       # Package configuration
├── requirements.txt     # Dependencies
├── README.md           # Documentation
└── run_pricing_aggregation_etl.py  # Example usage script
```

## Usage Example

```python
from PricingAggregationETL import PricingAggregationETL
from zervedataplatform.abstractions.types.enumerations.PipelineStage import PipelineStage
from zervedataplatform.pipeline.Pipeline import DataPipeline
from zervedataplatform.utils.Utility import Utility
from datetime import datetime

# Load configuration
config = Utility.read_in_json_file("configs/pricing_aggregation_configuration.json")

# Initialize ETL
etl = PricingAggregationETL(
    name="Pricing Aggregation ETL",
    pipeline_run_id="my-etl-run",
    default_configs=config,
    run_datestamp=datetime.now().strftime("%Y%m%d")
)

# Create pipeline
pipeline = DataPipeline()
pipeline.add_to_pipeline(etl)

# Execute pipeline stages
pipeline.run_only_pipeline(PipelineStage.initialize_task)
pipeline.run_only_pipeline(PipelineStage.pre_validate_task)
pipeline.run_only_pipeline(PipelineStage.read_task)
pipeline.run_only_pipeline(PipelineStage.main_task)
pipeline.run_only_pipeline(PipelineStage.output_task)
```

## Development Installation

For development work, install with dev dependencies:

```bash
pip install -e ".[dev]"
```

This includes additional tools:
- pytest (testing)
- black (code formatting)
- flake8 (linting)
- mypy (type checking)

## Troubleshooting

### Import Errors

If you encounter import errors, ensure:
1. The package is installed: `pip list | grep PricingAggregationETL`
2. You're using the correct Python environment
3. All dependencies are installed: `pip install -r requirements.txt`

### Configuration Issues

Ensure your configuration files exist in the `configs/` directory:
- `pricing_aggregation_configuration.json`
- `ai_api_config.json`
- `run_config.json`
- `bronze_db_config.json`
- `silver_db_config.json`
- `cloud_config.json`

## Uninstall

```bash
pip uninstall PricingAggregationETL
```
