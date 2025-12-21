import unittest
import tempfile
import os
import shutil
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame

from PricingAggregationETL import (
    PricingAggregationETL,
    PreValidationPhase,
    SourceReadPhase,
    ProcessPhase,
    OutputPhase,
    STAGED_TABLE_PREFIX,
    get_llm_characteristics
)


class TestPricingAggregationETL(unittest.TestCase):
    """Test cases for PricingAggregationETL"""

    def setUp(self):
        """Set up test fixtures"""
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()

        # Mock config data (now passed directly as dicts)
        self.ai_config = {
            "provider": "ollama",
            "model_name": "llama3.2",
            "base_url": "http://localhost:11434"
        }
        self.run_config = {
            "pipeline_activity_log_path": os.path.join(self.temp_dir, "pipeline.json"),
            "source_path": "s3://test-bucket/source",
            "xform_path": "s3://test-bucket/xform"
        }
        self.db_config = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db"
        }
        self.cloud_config = {
            "spark_config": {
                "spark.sql.shuffle.partitions": "2"
            }
        }
        self.llm_config = {
            "system_prompt": "You are a Product Characteristics Extractor Tool.",
            "examples": "Example test data for LLM extraction"
        }

        # Product category characteristics config
        self.product_characteristics_config = [
            {
                "footwear": {
                    "color": {
                        "description": "The primary color or color combinations of the footwear.",
                        "is_multi": True,
                        "options": ["<open_ended>"]
                    },
                    "material": {
                        "description": "The primary material used in the construction of the footwear.",
                        "is_multi": True,
                        "options": ["leather", "mesh", "synthetic"]
                    }
                }
            }
        ]

        # Config passed directly as dicts (matching run_pricing_aggregation_etl.py structure)
        self.default_configs = {
            "ai_api_config": self.ai_config,
            "run_config": self.run_config,
            "db_config": self.db_config,
            "cloud_config": self.cloud_config,
            "llm_extractor_config": self.llm_config,
            "product_super_category_characteristics_config": self.product_characteristics_config
        }

    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir)

    @patch('PricingAggregationETL.PricingAggregationETL.PipelineRunConfig')
    @patch('PricingAggregationETL.PricingAggregationETL.LLMCharacteristicsExtractor')
    @patch('PricingAggregationETL.PricingAggregationETL.ETLUtilities')
    def test_initialization(self, mock_etl_util, mock_llm, mock_pipeline_config):
        """Test PricingAggregationETL initialization with updated config structure"""
        # Mock return values
        mock_pipeline_config.return_value.ID = "test_pipeline_id"
        mock_pipeline_config.return_value.run_config = self.run_config
        mock_pipeline_config.return_value.db_config = self.db_config
        mock_pipeline_config.return_value.ai_config = self.ai_config

        # Create ETL instance
        etl = PricingAggregationETL(
            name="test_etl",
            run_datestamp="20231201",
            default_configs=self.default_configs,
            pipeline_run_id="test_run_id"
        )

        # Assertions
        self.assertEqual(etl.name, "test_etl")
        self.assertEqual(etl.run_datestamp, "20231201")

        # Verify ETLUtilities was instantiated
        mock_etl_util.assert_called_once()

        # Verify LLMCharacteristicsExtractor was called with correct parameters
        mock_llm.assert_called_once_with(
            self.llm_config,
            self.product_characteristics_config,
            self.ai_config
        )


if __name__ == '__main__':
    unittest.main()