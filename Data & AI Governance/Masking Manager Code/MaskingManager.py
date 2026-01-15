from typing import Dict, List, Optional
from datetime import datetime

class MaskingManager:

    def __init__(self, spark, audit_enabled: bool = True, dry_run: bool = False):
        self.spark = spark
        self.audit_enabled = audit_enabled
        self.dry_run = dry_run

    def apply_mask(
        self,
        table: str,
        column: str,
        mask_function: str
    ):
        sql = f"""
        ALTER TABLE {table}
        ALTER COLUMN {column}
        SET MASK {mask_function}
        """

        self._execute(sql)

        self._audit(
            action="APPLY_MASK",
            table=table,
            column=column,
            mask=mask_function
        )

    def remove_mask(self, table: str, column: str):
        sql = f"""
        ALTER TABLE {table}
        ALTER COLUMN {column}
        DROP MASK
        """

        self._execute(sql)

        self._audit(
            action="REMOVE_MASK",
            table=table,
            column=column
        )


    def apply_bulk_masks(
        self,
        table: str,
        column_mask_map: Dict[str, str]
    ):
        """
        Example:
        {
            "email": "email_mask",
            "ssn": "ssn_mask"
        }
        """
        for column, mask_fn in column_mask_map.items():
            self.apply_mask(table, column, mask_fn)


    def apply_role_based_mask(
        self,
        table: str,
        column: str,
        allowed_roles: List[str],
        mask_expression: str = "'****'"
    ):

        role_condition = " OR ".join(
            [f"is_member('{role}')" for role in allowed_roles]
        )

        dynamic_mask = f"""
        CASE
            WHEN {role_condition} THEN {column}
            ELSE {mask_expression}
        END
        """

        self.apply_mask(table, column, dynamic_mask)


    def apply_mask_if_tagged(
        self,
        table: str,
        column: str,
        tag_key: str,
        tag_value: str,
        mask_function: str
    ):

        tags_df = self.spark.sql(
            f"DESCRIBE TABLE EXTENDED {table}"
        )

        tag_exists = (
            tags_df
            .filter(f"col_name = '{tag_key}' AND data_type = '{tag_value}'")
            .count()
        )

        if tag_exists > 0:
            self.apply_mask(table, column, mask_function)
        else:
            raise ValueError(
                f"Masking policy skipped: Tag {tag_key}={tag_value} not found"
            )


    def mask_email(self, table: str, column: str):
        self.apply_mask(
            table,
            column,
            "regexp_replace(email, '(^.).*(@.*$)', '$1****$2')"
        )

    def mask_ssn(self, table: str, column: str):
        self.apply_mask(
            table,
            column,
            "concat('XXX-XX-', right(ssn, 4))"
        )

    def mask_credit_card(self, table: str, column: str):
        self.apply_mask(
            table,
            column,
            "concat('XXXX-XXXX-XXXX-', right(card_number, 4))"
        )


    def show_masks(self, table: str):
        return self.spark.sql(f"DESCRIBE TABLE EXTENDED {table}")


    def _execute(self, sql: str):
        if self.dry_run:
            print(f"[DRY RUN] {sql}")
        else:
            self.spark.sql(sql)

    def _audit(self, action: str, table: str, column: str, mask: Optional[str] = None):
        if not self.audit_enabled:
            return

        audit_log = {
            "timestamp": datetime.utcnow().isoformat(),
            "action": action,
            "table": table,
            "column": column,
            "mask": mask
        }

        print(f"[MASKING AUDIT] {audit_log}")



# Simple Mask 

mask_mgr = MaskingManager(spark)

mask_mgr.apply_mask(
    table="finance.customers",
    column="email",
    mask_function="email_mask"
)

# Role-Based Masking

mask_mgr.apply_role_based_mask(
    table="finance.customers",
    column="ssn",
    allowed_roles=["risk_admin", "compliance_team"]
)


# Bulk Masking 

mask_mgr.apply_bulk_masks(
    table="finance.customers",
    column_mask_map={
        "email": "email_mask",
        "ssn": "ssn_mask",
        "card_number": "cc_mask"
    }
)

# Tag-Driven Masking

mask_mgr.apply_mask_if_tagged(
    table="finance.customers",
    column="email",
    tag_key="classification",
    tag_value="PII",
    mask_function="email_mask"
)

