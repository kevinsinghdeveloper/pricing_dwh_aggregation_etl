from datetime import datetime

from zervedataplatform.abstractions.types.enumerations.PipelineStage import PipelineStage
from PricingAggregationETL import PricingAggregationETL
from zervedataplatform.pipeline.Pipeline import DataPipeline
from zervedataplatform.utils.Utility import Utility

default_configurations = {

}

app = PricingAggregationETL(name="Pricing Aggregation ETL",
                               pipeline_run_id="pricing-agg-etl",
                               default_configs=default_configurations,
                               run_datestamp=datetime.now().strftime("%Y%m%d"))

data_pipeline = DataPipeline()
data_pipeline.add_to_pipeline(app)

data_pipeline.run_only_pipeline(PipelineStage.initialize_task)
data_pipeline.run_only_pipeline(PipelineStage.pre_validate_task)
data_pipeline.run_only_pipeline(PipelineStage.read_task)
data_pipeline.run_only_pipeline(PipelineStage.main_task)
data_pipeline.run_only_pipeline(PipelineStage.output_task)