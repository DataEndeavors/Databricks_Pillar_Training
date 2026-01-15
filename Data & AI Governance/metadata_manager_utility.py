import logging
from typing import Dict, List, Optional


class MetadataManager:

    def __init__(self, spark, dry_run: bool = False, logger: Optional[logging.Logger] = None):
        self.spark = spark
        self.dry_run = dry_run
        self.logger = logger or self._default_logger()


    def _default_logger(self):
        logger = logging.getLogger("MetadataManager")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def _execute_sql(self, sql: str):
        self.logger.info(f"Executing SQL:\n{sql}")
        if not self.dry_run:
            self.spark.sql(sql)

    def _escape(self, value: str) -> str:
        return value.replace("'", "''")

    def _validate_table(self, table: str):
        if not table or "." not in table:
            raise ValueError("Table must be fully qualified: catalog.schema.table")


    def add_table_comment(self, table: str, comment: str):
        self._validate_table(table)
        comment = self._escape(comment)

        sql = f"""
        COMMENT ON TABLE {table}
        IS '{comment}'
        """
        self._execute_sql(sql)

    def add_table_tags(self, table: str, tags: Dict[str, str]):
        self._validate_table(table)

        if not tags:
            self.logger.warning("No tags provided")
            return

        tag_sql = ", ".join(
            [f"'{self._escape(k)}'='{self._escape(v)}'" for k, v in tags.items()]
        )

        sql = f"""
        ALTER TABLE {table}
        SET TAGS ({tag_sql})
        """
        self._execute_sql(sql)

    def remove_table_tags(self, table: str, tag_keys: List[str]):
        self._validate_table(table)

        tag_sql = ", ".join([f"'{self._escape(k)}'" for k in tag_keys])

        sql = f"""
        ALTER TABLE {table}
        UNSET TAGS ({tag_sql})
        """
        self._execute_sql(sql)


    def add_column_comment(self, table: str, column: str, comment: str):
        self._validate_table(table)
        comment = self._escape(comment)

        sql = f"""
        COMMENT ON COLUMN {table}.{column}
        IS '{comment}'
        """
        self._execute_sql(sql)

    def add_column_tags(
        self,
        table: str,
        column: str,
        tags: Dict[str, str]
    ):
        self._validate_table(table)

        tag_sql = ", ".join(
            [f"'{self._escape(k)}'='{self._escape(v)}'" for k, v in tags.items()]
        )

        sql = f"""
        ALTER TABLE {table}
        ALTER COLUMN {column}
        SET TAGS ({tag_sql})
        """
        self._execute_sql(sql)


    def apply_data_classification(
        self,
        table: str,
        classification: str,
        regulation: Optional[str] = None
    ):
        tags = {"data_classification": classification}
        if regulation:
            tags["regulation"] = regulation

        self.add_table_tags(table, tags)

    def assign_data_owner(self, table: str, owner: str):
        self.add_table_tags(table, {"data_owner": owner})

    def apply_standard_governance(
        self,
        table: str,
        domain: str,
        owner: str,
        classification: str,
        regulation: Optional[str] = None
    ):
        tags = {
            "domain": domain,
            "data_owner": owner,
            "data_classification": classification
        }
        if regulation:
            tags["regulation"] = regulation

        self.add_table_tags(table, tags)


    def bulk_apply_comments(
        self,
        table: str,
        table_comment: Optional[str] = None,
        column_comments: Optional[Dict[str, str]] = None
    ):
        if table_comment:
            self.add_table_comment(table, table_comment)

        if column_comments:
            for column, comment in column_comments.items():
                self.add_column_comment(table, column, comment)

    def bulk_apply_tags(
        self,
        table: str,
        table_tags: Optional[Dict[str, str]] = None,
        column_tags: Optional[Dict[str, Dict[str, str]]] = None
    ):
        if table_tags:
            self.add_table_tags(table, table_tags)

        if column_tags:
            for column, tags in column_tags.items():
                self.add_column_tags(table, column, tags)
