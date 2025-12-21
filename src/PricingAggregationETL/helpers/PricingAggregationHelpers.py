from zervedataplatform.utils.ETLUtilities import ETLUtilities
from zervedataplatform.model_transforms.db.PipelineRunConfig import PipelineRunConfig

class PricingAggregationETLUtilities(ETLUtilities):
    def __init__(self, pipeline_run_config: PipelineRunConfig):
        super().__init__(pipeline_run_config)

        # override init
        self.__spark_cloud_manager = self._ETLUtilities__spark_source_db_manager
        self.__spark_source_db_manager = self._ETLUtilities__spark_dest_db_manager