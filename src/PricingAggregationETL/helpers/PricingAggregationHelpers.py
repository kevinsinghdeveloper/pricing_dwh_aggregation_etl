from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, sum as spark_sum, avg, count, max as spark_max, min as spark_min
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

    @staticmethod
    def add_aggregation_level(df: DataFrame, level_config: dict, level_name: str):
        level_num = level_config['level_num']

        # aggregate rows. then assign hierarchy columns
        agg_dims = level_config['agg_dims']
        measure_cols = level_config['measure_cols']  # dict: {output_col: {agg_method, base_measure}}

        # Map aggregation method names to PySpark functions
        agg_func_map = {
            'sum': spark_sum,
            'mean': avg,
            'avg': avg,
            'count': count,
            'max': spark_max,
            'min': spark_min
        }

        # Get all original columns (excluding hierarchy columns) to maintain schema
        original_cols = [col for col in df.columns if col not in ['hier_level', 'hier_level_name']]

        # Create aggregation expressions based on measure_cols config
        agg_exprs = []
        for output_col_name, measure_config in measure_cols.items():
            agg_method = measure_config['agg_method']
            base_measure = measure_config['base_measure']

            if agg_method in agg_func_map:
                agg_func = agg_func_map[agg_method]
                agg_exprs.append(agg_func(base_measure).alias(output_col_name))
            else:
                raise ValueError(f"Unsupported aggregation method: {agg_method}")

        # Group by dimensions and aggregate
        agg_df = df.groupBy(*agg_dims).agg(*agg_exprs)

        # Add missing columns as NULL to maintain consistent schema across all levels
        for col in original_cols:
            if col not in agg_df.columns:
                agg_df = agg_df.withColumn(col, lit(None).cast("string"))

        # Assign hierarchy columns
        agg_df = agg_df.withColumn('hier_level', lit(level_num))
        agg_df = agg_df.withColumn('hier_level_name', lit(level_name))

        return agg_df