from zervedataplatform.model_transforms.db.PipelineRunConfig import PipelineRunConfig
from zervedataplatform.pipeline.Pipeline import DataConnectorBase, FuncDataPipe, FuncPipelineStep
from zervedataplatform.utils.Utility import Utility
from PricingAggregationETL.helpers.PricingAggregationHelpers import PricingAggregationETLUtilities


class PreValidationPhase:
    VALIDATE_SOURCE_TABLES = "validate_source_tables"
    class PreValidationVariables:
        SOURCE_TABLES = "source_tables"

class PricingAggregationETL(DataConnectorBase):
    default_hierarchy_level_map = {
        'market': {
            "level_num": 0,
            "agg_dims": ['market', 'priced_date']
        },
        'category': {
            "level_num": 1,
            "agg_dims": ['market', 'category', 'priced_date']
        },
    }
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
        self.default_hierarchy_level_map = default_configs.get("default_hierarchy_level_map", self.default_hierarchy_level_map)

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

        # check source
        if not self.__etl_util.check_db_connection():
            Utility.error_log(f"DB connection check failed for source db")
            raise Exception(f"DB connection check failed for source db")

        if not self.__etl_util.check_db_connection(use_dest_db=True):
            Utility.error_log(f"DB connection check failed for destination db")
            raise Exception(f"DB connection check failed for destination db")

        Utility.log("Validating source tables...")

        if not tables:
            Utility.error_log(f"Source tables not found...")

            raise Exception("Source tables not found...")

        Utility.log(f"Found tables! {tables}")

        def_cols = list(self.__default_product_pricing_schema_map.keys())

        table_col_map = {}
        for table in tables:
            Utility.log(f"Validating table: {table}")

            table_cols = self.__etl_util.get_table_columns(table)

            missing_required_cols = set(def_cols) - set(table_cols)
            if missing_required_cols:
                Utility.warning_log(f"Table {table} is missing required columns: {missing_required_cols}.")

            table_col_map[table] = {"columns": table_cols}

        if not table_col_map:
            Utility.error_log(f"Tables not found...")
            raise Exception("Tables not found...")

        self.get_pipeline_activity_logger().add_pipeline_variable(
            task_name=PreValidationPhase.VALIDATE_SOURCE_TABLES,
            variable_name=PreValidationPhase.PreValidationVariables.SOURCE_TABLES,
            variable_type="dict",
            variable_value=table_col_map)

    def __move_data_to_destination_db(self):
        source_tables = self.get_pipeline_activity_logger().get_pipeline_variable(
            task_name=PreValidationPhase.VALIDATE_SOURCE_TABLES,
            variable_name=PreValidationPhase.PreValidationVariables.SOURCE_TABLES)

        for table in source_tables:
            Utility.log(f"Moving table: {table}")

            # TODO
            self.__etl_util.move_table(table)

    def __clean_and_convert_data(self):
        pass

    def __aggregate_data_levels(self):
        pass

        # TODO
        # read df
        # convert dates to month
        # aggregate merchant | category | date
        # merchant | date (level 1) -- add row
        # merchant | category | date (level 2) -- add row


        # TODO IMPROVE etl utils
        # 1. add check db
        # 2. add move_table_to_destination_db or have to and from. migrate_table(table, config{})
