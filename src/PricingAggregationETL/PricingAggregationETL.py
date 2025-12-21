from zervedataplatform.model_transforms.db.PipelineRunConfig import PipelineRunConfig
from zervedataplatform.pipeline.Pipeline import DataConnectorBase, FuncDataPipe, FuncPipelineStep
from zervedataplatform.utils.Utility import Utility
from PricingAggregationETL.helpers.PricingAggregationHelpers import PricingAggregationETLUtilities


class PreValidationPhase:
    VALIDATE_SOURCE_TABLES = "validate_source_tables"
    class PreValidationVariables:
        SOURCE_TABLES = "source_tables"

class PricingAggregationETL(DataConnectorBase):
    def __init__(self, name: str, run_datestamp: str, default_configs: dict, pipeline_run_id: str = None):
        super().__init__(name, run_datestamp)

        pipeline_run_config = PipelineRunConfig(ai_config=default_configs["ai_api_config"],
                                                run_config=default_configs['run_config'],
                                                db_config=default_configs['silver_db_config'],
                                                dest_db_config=default_configs['aggregated_db_config'],
                                                cloud_config=default_configs.get('cloud_config'),
                                                ID=pipeline_run_id)

        self.__pipeline_id = pipeline_run_config.ID
        self.__config = pipeline_run_config.run_config
        self.__db_config = pipeline_run_config.db_config

        self.__default_product_pricing_schema_map = default_configs.get("default_product_pricing_schema_map")

        self.__etl_util = PricingAggregationETLUtilities(pipeline_run_config)
        pipeline_log_path = self.__config["pipeline_activity_log_path"]

        self.init_pipeline_activity_logger(pipeline_activity_log_path=pipeline_log_path)
        self.__pipeline_activity_data = self.get_pipeline_activity_logger().get_pipeline_activity_log_contents()

        self._pre_validation_pipeline = FuncDataPipe(name_of_pipe="*** Pre Validate Files")
        self._source_read_pipeline = FuncDataPipe(name_of_pipe="*** Source reader")
        self._process_task_pipeline = FuncDataPipe(name_of_pipe="*** Execute task")
        self._output_pipeline = FuncDataPipe(name_of_pipe="*** Output task")

    def run_initialize_task(self) -> None:
        Utility.log("Initializing...")

        self._pre_validation_pipeline.add_to_pipeline(
            FuncPipelineStep(
                name_of_step=PreValidationPhase.VALIDATE_SOURCE_TABLES,
                func=self.__validate_source_tables
            )
        )

    def run_pre_validate_task(self) -> None:
        try:
            self._pre_validation_pipeline.run_pipeline()
        except Exception as e:
            Utility.error_log(f"Error running read task: {e}")

    def run_read_task(self) -> None:
        try:
            self._source_read_pipeline.run_pipeline()
        except Exception as e:
            Utility.error_log(f"Error running read task: {e}")

    def run_main_task(self) -> None:
        try:
            self._process_task_pipeline.run_pipeline()
        except Exception as e:
            Utility.error_log(f"Error running main task: {e}")

    def run_output_task(self) -> None:
        try:
            self._output_pipeline.run_pipeline()
        except Exception as e:
            Utility.error_log(f"Error running output task: {e}")

    def __validate_source_tables(self):
        # read all tables from source
        tables = self.__etl_util.get_all_db_tables()

        Utility.log("Checking source and destination db connections...")

        # check here

        Utility.log("Validating source tables...")

        if not tables:
            Utility.error_log(f"Source tables not found...")

            raise Exception("Source tables not found...")

        Utility.log(f"Found tables! {tables}")

        for table in tables:
            Utility.log(f"Validating table: {table}")

            table_cols = self.__etl_util.get_table_columns(table)

            pass