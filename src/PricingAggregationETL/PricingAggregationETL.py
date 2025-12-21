from zervedataplatform.connectors.ai.llm_characteristics_extractor import LLMCharacteristicsExtractor
from zervedataplatform.model_transforms.db.PipelineRunConfig import PipelineRunConfig
from zervedataplatform.pipeline.Pipeline import DataConnectorBase, FuncDataPipe
from zervedataplatform.utils.ETLUtilities import ETLUtilities
from zervedataplatform.utils.Utility import Utility

DEFAULT_LLM_TIMEOUT = 45

class PreValidationPhase:
    CHECK_FILES_FROM_LATEST = "check_latest_files"
    class PreValidationVariables:
        LATEST_SOURCE_FOLDER = "latest_source_folder"

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

        llm_extractor_config = default_configs.get("llm_extractor_config")
        product_super_category_characteristics_config = default_configs.get("product_super_category_characteristics_config")
        self.__default_product_pricing_schema_map = default_configs.get("default_product_pricing_schema_map")

        self.__llm_extractor = None
        self.__product_super_category_characteristics_columns = {}
        if llm_extractor_config and product_super_category_characteristics_config:
            self.__llm_extractor = LLMCharacteristicsExtractor(llm_extractor_config,
                                                               product_super_category_characteristics_config,
                                                               pipeline_run_config.ai_config)

            for super_category_config in product_super_category_characteristics_config:
                for super_category, config in super_category_config.items():
                    self.__product_super_category_characteristics_columns[super_category] = config

        else:
            raise Exception(f"No llm_extractor_config or product_super_category_characteristics_config")

        self.__etl_util = ETLUtilities(pipeline_run_config)
        pipeline_log_path = self.__config["pipeline_activity_log_path"]

        self.init_pipeline_activity_logger(pipeline_activity_log_path=pipeline_log_path)
        self.__pipeline_activity_data = self.get_pipeline_activity_logger().get_pipeline_activity_log_contents()

        self._pre_validation_pipeline = FuncDataPipe(name_of_pipe="*** Pre Validate Files")
        self._source_read_pipeline = FuncDataPipe(name_of_pipe="*** Source reader")
        self._process_task_pipeline = FuncDataPipe(name_of_pipe="*** Execute task")
        self._output_pipeline = FuncDataPipe(name_of_pipe="*** Output task")

    def run_initialize_task(self) -> None:
        Utility.log("Initializing...")

        # self._pre_validation_pipeline.add_to_pipeline(
        #     FuncPipelineStep(
        #         name_of_step=PreValidationPhase.CHECK_FILES_FROM_LATEST,
        #         func=self.__prevalidate_source_files
        #     )
        # )

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
