"""Pydantic models for API responses."""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field

from .. import __version__


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


# --- Cluster Models ---


class ClusterState(str, Enum):
    """Cluster state enumeration."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RESTARTING = "RESTARTING"
    RESIZING = "RESIZING"
    TERMINATING = "TERMINATING"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"
    UNKNOWN = "UNKNOWN"


class ClusterSource(str, Enum):
    """Cluster source enumeration."""
    UI = "UI"
    API = "API"
    JOB = "JOB"
    MODELS = "MODELS"
    PIPELINE = "PIPELINE"
    PIPELINE_MAINTENANCE = "PIPELINE_MAINTENANCE"
    SQL = "SQL"


class AutoScaleConfig(BaseModel):
    """Autoscale configuration."""
    min_workers: int
    max_workers: int


class ClusterSummary(BaseModel):
    """Summary view of a cluster."""
    cluster_id: str
    cluster_name: str
    state: ClusterState
    creator_user_name: str | None = None
    node_type_id: str | None = None
    driver_node_type_id: str | None = None
    num_workers: int | None = None
    autoscale: AutoScaleConfig | None = None
    spark_version: str | None = None
    cluster_source: ClusterSource | None = None
    start_time: datetime | None = None
    last_activity_time: datetime | None = None
    uptime_minutes: int = 0
    estimated_dbu_per_hour: float = 0.0


class ClusterDetail(ClusterSummary):
    """Detailed view of a cluster."""
    terminated_time: datetime | None = None
    termination_reason: str | None = None
    state_message: str | None = None
    default_tags: dict[str, str] = Field(default_factory=dict)
    custom_tags: dict[str, str] = Field(default_factory=dict)
    aws_attributes: dict | None = None
    azure_attributes: dict | None = None
    gcp_attributes: dict | None = None
    spark_conf: dict[str, str] = Field(default_factory=dict)
    spark_env_vars: dict[str, str] = Field(default_factory=dict)
    init_scripts: list[dict] = Field(default_factory=list)
    cluster_log_conf: dict | None = None
    policy_id: str | None = None
    enable_elastic_disk: bool | None = None
    disk_spec: dict | None = None
    single_user_name: str | None = None
    data_security_mode: str | None = None


class ClusterEvent(BaseModel):
    """Cluster event."""
    cluster_id: str
    timestamp: datetime
    event_type: str
    details: dict = Field(default_factory=dict)


class ClusterEventsResponse(BaseModel):
    """Response for cluster events."""
    events: list[ClusterEvent]
    next_page_token: str | None = None
    total_count: int


class ClusterActionResponse(BaseModel):
    """Response for cluster actions."""
    success: bool
    message: str
    cluster_id: str


# --- Metrics Models ---


class ClusterMetricsSummary(BaseModel):
    """Summary of cluster metrics."""
    total_clusters: int
    running_clusters: int
    pending_clusters: int
    terminated_clusters: int
    total_running_workers: int
    estimated_hourly_dbu: float


class IdleClusterAlert(BaseModel):
    """Alert for an idle cluster."""
    cluster_id: str
    cluster_name: str
    idle_duration_minutes: int
    estimated_wasted_dbu: float
    recommendation: str


class OptimizationRecommendation(BaseModel):
    """Recommendation for cluster optimization."""
    cluster_id: str
    cluster_name: str
    issue: str
    recommendation: str
    potential_savings: str
    priority: str = "medium"  # low, medium, high


# --- Billing Models ---


class BillingSummary(BaseModel):
    """Summary of billing information."""
    total_dbu: float
    estimated_cost_usd: float
    period_start: datetime
    period_end: datetime
    currency: str = "USD"


class ClusterBillingUsage(BaseModel):
    """Billing usage for a specific cluster."""
    cluster_id: str
    cluster_name: str | None = None
    total_dbu: float
    estimated_cost_usd: float
    usage_date_start: datetime
    usage_date_end: datetime


class BillingTrend(BaseModel):
    """Daily billing trend data point."""
    date: datetime
    dbu: float
    estimated_cost_usd: float


class TopConsumer(BaseModel):
    """Top consuming cluster."""
    cluster_id: str
    cluster_name: str | None = None
    total_dbu: float
    estimated_cost_usd: float
    percentage_of_total: float


# --- Policy Models ---


class ClusterPolicySummary(BaseModel):
    """Summary view of a cluster policy."""
    policy_id: str
    name: str
    definition: str | None = None
    description: str | None = None
    creator_user_name: str | None = None
    created_at_timestamp: datetime | None = None
    is_default: bool = False


class ClusterPolicyDetail(ClusterPolicySummary):
    """Detailed view of a cluster policy."""
    definition_json: dict = Field(default_factory=dict)
    max_clusters_per_user: int | None = None
    policy_family_id: str | None = None
    policy_family_definition_overrides: str | None = None


class PolicyUsage(BaseModel):
    """Policy usage information."""
    policy_id: str
    policy_name: str
    cluster_count: int
    clusters: list[ClusterSummary] = Field(default_factory=list)
