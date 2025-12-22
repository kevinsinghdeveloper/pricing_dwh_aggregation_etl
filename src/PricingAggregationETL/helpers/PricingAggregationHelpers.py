from zervedataplatform.utils.ETLUtilities import ETLUtilities
from zervedataplatform.model_transforms.db.PipelineRunConfig import PipelineRunConfig

class PricingAggregationETLUtilities(ETLUtilities):
    def __init__(self, pipeline_run_config: PipelineRunConfig):
        super().__init__(pipeline_run_config)

        # override init
        self.__spark_source_db_manager = self._ETLUtilities__spark_source_db_manager
        self.__spark_dest_db_manager = self._ETLUtilities__spark_dest_db_manager

    # TODO move to platform tools
    def check_db_connection(self, use_dest_db=False):
        db_manager = self.__spark_dest_db_manager if use_dest_db else self.__spark_source_db_manager

        # TODO fix this
        return db_manager.check_db_status()