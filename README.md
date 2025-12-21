# PricingAggregationETL

Pricing data aggregation ETL pipeline for data warehouse analytics.

## Overview

PricingAggregationETL is a comprehensive ETL (Extract, Transform, Load) pipeline designed to aggregate and process pricing data for data warehouse analytics. It collects pricing information from various sources and transforms it for analytical processing.

## Features

- **Data Aggregation**: Aggregates pricing data from multiple sources for comprehensive analysis
- **Cloud Integration**: Supports AWS and Google Cloud services for data storage and processing
- **Flexible Pipeline**: Modular pipeline architecture with distinct phases for validation, reading, processing, and output
- **Data Staging**: Automated staging tables for processed data into bronze and silver layers
- **DWH Integration**: Designed for data warehouse analytics workflows

## Installation

### From Git Repository

```bash
pip install git+https://github.com/zerveme/PricingAggregationETL.git
```

### From Local Source

```bash
# Clone the repository
git clone <repository-url>
cd PricingAggregationETL

# Install in development mode
pip install -e .
```

### Development Installation

```bash
# Install with development dependencies
pip install -e ".[dev]"
```

## Usage

### Basic Usage

```python
from PricingAggregationETL import PricingAggregationETL
from zervedataplatform.abstractions.types.enumerations.PipelineStage import PipelineStage
from zervedataplatform.pipeline.Pipeline import DataPipeline
from zervedataplatform.utils.Utility import Utility
from datetime import datetime

# Load configuration
default_configurations = Utility.read_in_json_file("configs/pricing_aggregation_configuration.json")

# Initialize the ETL
app = PricingAggregationETL(
    name="Pricing Aggregation ETL",
    pipeline_run_id="pricing-agg-etl-20250120",
    default_configs=default_configurations,
    run_datestamp=datetime.now().strftime("%Y%m%d")
)

# Create and run pipeline
data_pipeline = DataPipeline()
data_pipeline.add_to_pipeline(app)

# Run pipeline stages
data_pipeline.run_only_pipeline(PipelineStage.initialize_task)
data_pipeline.run_only_pipeline(PipelineStage.pre_validate_task)
data_pipeline.run_only_pipeline(PipelineStage.read_task)
data_pipeline.run_only_pipeline(PipelineStage.main_task)
data_pipeline.run_only_pipeline(PipelineStage.output_task)
```

### Configuration

The ETL requires a configuration JSON file with the following structure:

```json
{
  "ai_api_config": "path/to/ai_config.json",
  "run_config": "path/to/run_config.json",
  "bronze_db_config": "path/to/bronze_db_config.json",
  "silver_db_config": "path/to/silver_db_config.json",
  "cloud_config": "path/to/cloud_config.json",
  "llm_extractor_config": "path/to/llm_extractor_config.json",
  "product_super_category_characteristics_config": [],
  "default_product_pricing_schema_map": {}
}
```

## Pipeline Stages

The ETL pipeline consists of five main stages:

1. **Initialize Task**: Sets up the pipeline steps and configuration
2. **Pre-Validate Task**: Validates source files and data consistency
3. **Read Task**: Moves source data to transformation location
4. **Main Task**: Performs data aggregation and transformations
5. **Output Task**: Stages processed data into database tables (bronze and silver layers)

## Available Classes and Functions

- `PricingAggregationETL`: Main ETL class
- `PreValidationPhase`: Constants and variables for pre-validation
- `SourceReadPhase`: Constants and variables for source reading
- `ProcessPhase`: Constants and variables for processing
- `OutputPhase`: Constants and variables for output

## Requirements

All dependencies are managed through the `zervedataplatform` package, which includes:

- Data processing (pandas, numpy, PySpark)
- Cloud services (AWS, Google Cloud)
- Database connectivity
- Pipeline orchestration utilities

## Development

### Running Tests

```bash
pytest
```

### Code Formatting

```bash
black .
```

### Linting

```bash
flake8
```

## License

Copyright (c) Zerveme LLC

## Support

For issues and questions, please contact noreply@zerveme.com
