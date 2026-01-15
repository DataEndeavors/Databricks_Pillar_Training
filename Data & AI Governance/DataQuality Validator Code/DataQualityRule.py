from typing import Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


class DataQualityRule:
    def __init__(
        self,
        name: str,
        condition: str,
        severity: str = "ERROR",
        dimension: str = "VALIDITY",
        description: Optional[str] = None
    ):
        self.name = name
        self.condition = condition
        self.severity = severity.upper()
        self.dimension = dimension.upper()
        self.description = description


class DataQualityValidator:

    def __init__(self, spark, fail_fast: bool = False):
        self.spark = spark
        self.fail_fast = fail_fast


    def validate(
        self,
        df: DataFrame,
        rules: List[DataQualityRule]
    ) -> Dict[str, DataFrame]:


        validation_df = df
        report_rows = []

        for rule in rules:
            passed_df = validation_df.filter(rule.condition)
            failed_df = validation_df.filter(f"NOT ({rule.condition})")

            pass_count = passed_df.count()
            fail_count = failed_df.count()

            report_rows.append(
                (
                    rule.name,
                    rule.dimension,
                    rule.severity,
                    rule.description,
                    pass_count,
                    fail_count
                )
            )

            if rule.severity == "ERROR":
                validation_df = passed_df
                if self.fail_fast and fail_count > 0:
                    raise Exception(f"Data quality check failed: {rule.name}")

        report_df = self.spark.createDataFrame(
            report_rows,
            schema=[
                "rule_name",
                "dimension",
                "severity",
                "description",
                "passed_records",
                "failed_records"
            ]
        )

        invalid_df = df.subtract(validation_df)

        return {
            "valid_df": validation_df,
            "invalid_df": invalid_df,
            "report_df": report_df
        }


    @staticmethod
    def not_null(column: str, **kwargs) -> DataQualityRule:
        return DataQualityRule(
            name=f"{column}_not_null",
            condition=f"{column} IS NOT NULL",
            dimension="COMPLETENESS",
            **kwargs
        )

    @staticmethod
    def value_in_range(column: str, min_val, max_val, **kwargs) -> DataQualityRule:
        return DataQualityRule(
            name=f"{column}_range_check",
            condition=f"{column} BETWEEN {min_val} AND {max_val}",
            dimension="VALIDITY",
            **kwargs
        )

    @staticmethod
    def allowed_values(column: str, values: List[str], **kwargs) -> DataQualityRule:
        values_str = ",".join([f"'{v}'" for v in values])
        return DataQualityRule(
            name=f"{column}_allowed_values",
            condition=f"{column} IN ({values_str})",
            dimension="VALIDITY",
            **kwargs
        )


    def apply_standard_financial_rules(self) -> List[DataQualityRule]:
        return [
            self.not_null("account_id", severity="ERROR"),
            self.not_null("customer_id", severity="ERROR"),
            self.value_in_range("balance", 0, 100000000, severity="WARN"),
            self.allowed_values(
                "account_status",
                ["ACTIVE", "INACTIVE", "CLOSED"],
                severity="ERROR"
            )
        ]


dq = DataQualityValidator(spark)

rules = [
    dq.not_null("account_id", description="Account ID must be present"),
    dq.not_null("customer_id"),
    dq.value_in_range("balance", 0, 1_000_000, severity="WARN"),
    dq.allowed_values("country", ["IN", "US", "UK"])
]

result = dq.validate(df, rules)

valid_df = result["valid_df"]
invalid_df = result["invalid_df"]
report_df = result["report_df"]
