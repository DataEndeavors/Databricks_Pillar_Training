from typing import List, Dict, Optional
from datetime import datetime

class SecurityManager:

    def __init__(self, spark, audit_enabled: bool = True):
        self.spark = spark
        self.audit_enabled = audit_enabled

    def grant(
        self,
        object_type: str,
        object_name: str,
        principal: str,
        privileges: List[str]
    ):
        for privilege in privileges:
            self.spark.sql(
                f"GRANT {privilege} ON {object_type} {object_name} TO `{principal}`"
            )

        self._audit(
            action="GRANT",
            object_type=object_type,
            object_name=object_name,
            principal=principal,
            privileges=privileges
        )

    def revoke(
        self,
        object_type: str,
        object_name: str,
        principal: str,
        privileges: List[str]
    ):
        for privilege in privileges:
            self.spark.sql(
                f"REVOKE {privilege} ON {object_type} {object_name} FROM `{principal}`"
            )

        self._audit(
            action="REVOKE",
            object_type=object_type,
            object_name=object_name,
            principal=principal,
            privileges=privileges
        )


    def grant_select(self, table: str, group: str):
        self.grant(
            object_type="TABLE",
            object_name=table,
            principal=group,
            privileges=["SELECT"]
        )

    def grant_read_only(self, table: str, group: str):
        self.grant(
            object_type="TABLE",
            object_name=table,
            principal=group,
            privileges=["SELECT"]
        )

    def grant_read_write(self, table: str, group: str):
        self.grant(
            object_type="TABLE",
            object_name=table,
            principal=group,
            privileges=["SELECT", "INSERT", "UPDATE"]
        )


    def bulk_grant(
        self,
        object_type: str,
        object_name: str,
        role_privilege_map: Dict[str, List[str]]
    ):
        """
        Example:
        {
          "finance_readers": ["SELECT"],
          "finance_writers": ["SELECT", "INSERT"]
        }
        """
        for role, privileges in role_privilege_map.items():
            self.grant(object_type, object_name, role, privileges)


    def apply_rbac_policy(self, table: str, domain: str):
        """
        Standard enterprise RBAC pattern
        """
        policies = {
            f"{domain}_readers": ["SELECT"],
            f"{domain}_writers": ["SELECT", "INSERT", "UPDATE"],
            f"{domain}_admins": ["ALL PRIVILEGES"]
        }

        self.bulk_grant("TABLE", table, policies)


    def grant_if_tag_matches(
        self,
        table: str,
        group: str,
        required_tag_key: str,
        required_tag_value: str,
        privilege: str = "SELECT"
    ):

        tags_df = self.spark.sql(f"DESCRIBE TABLE EXTENDED {table}")

        tag_match = tags_df.filter(
            f"col_name = '{required_tag_key}' AND data_type = '{required_tag_value}'"
        ).count()

        if tag_match > 0:
            self.grant("TABLE", table, group, [privilege])
        else:
            raise PermissionError(
                f"Security policy violation: Tag {required_tag_key}={required_tag_value} missing"
            )


    def show_grants(self, object_type: str, object_name: str):
        return self.spark.sql(
            f"SHOW GRANTS ON {object_type} {object_name}"
        )

    def snapshot_permissions(self, object_type: str, object_name: str):
        df = self.show_grants(object_type, object_name)
        return (
            df
            .withColumn("snapshot_time", lit(datetime.utcnow().isoformat()))
        )


    def _audit(self, action, object_type, object_name, principal, privileges):
        if not self.audit_enabled:
            return

        audit_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "action": action,
            "object_type": object_type,
            "object_name": object_name,
            "principal": principal,
            "privileges": ",".join(privileges)
        }

        print(f"[SECURITY AUDIT] {audit_record}")


sec = SecurityManager(spark)

sec.grant_read_only(
    table="finance.curated_transactions",
    group="finance_readers"
)

sec.apply_rbac_policy(
    table="finance.curated_transactions",
    domain="finance"
)

sec.grant_if_tag_matches(
    table="finance.curated_transactions",
    group="pii_analysts",
    required_tag_key="classification",
    required_tag_value="PII"
)
