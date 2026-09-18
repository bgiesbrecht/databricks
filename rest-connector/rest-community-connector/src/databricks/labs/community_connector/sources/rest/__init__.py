"""REST source connector."""

from databricks.labs.community_connector.sources.rest.rest import (
    RestLakeflowConnect,
)

from databricks.labs.community_connector.sparkpds import LakeflowSource


class RestDataSource(LakeflowSource):
    _lakeflow_connect_cls = RestLakeflowConnect


__all__ = ["RestLakeflowConnect", "RestDataSource"]
