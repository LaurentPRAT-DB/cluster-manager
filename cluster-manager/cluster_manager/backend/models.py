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


# --- Optimization Models ---


class ClusterType(str, Enum):
    """Cluster type classification based on source."""
    JOB = "JOB"
    INTERACTIVE = "INTERACTIVE"
    SQL = "SQL"
    PIPELINE = "PIPELINE"
    MODELS = "MODELS"


class ClusterUtilizationMetric(BaseModel):
    """Daily utilization metrics for a cluster."""
    cluster_id: str
    cluster_name: str
    metric_date: datetime
    cluster_type: ClusterType

    # Capacity metrics
    worker_count: int
    potential_dbu_per_hour: float

    # Actual usage metrics
    actual_dbu: float
    uptime_hours: float

    # Efficiency metrics (0-100)
    efficiency_score: float

    # Activity metrics (type-specific)
    job_run_count: int | None = None
    unique_users: int | None = None

    # Computed status
    is_oversized: bool = False
    is_underutilized: bool = False


class OversizedClusterAnalysis(BaseModel):
    """Analysis of an oversized cluster with recommendations."""
    cluster_id: str
    cluster_name: str
    cluster_type: ClusterType
    current_workers: int
    avg_efficiency_score: float
    avg_daily_dbu: float
    recommended_workers: int
    potential_dbu_savings: float
    potential_cost_savings: float


class JobClusterRecommendation(BaseModel):
    """Recommendation to move jobs to an oversized cluster."""
    source_cluster_id: str
    source_cluster_name: str
    target_cluster_id: str
    target_cluster_name: str
    job_count: int
    reason: str
    estimated_savings: str


class UserConsolidationRecommendation(BaseModel):
    """Recommendation to consolidate users across clusters."""
    cluster_ids: list[str]
    cluster_names: list[str]
    total_users: int
    total_current_workers: int
    recommended_workers: int
    reason: str
    estimated_savings: str


class ScheduleOptimizationRecommendation(BaseModel):
    """Recommendation to optimize cluster start/stop times."""
    cluster_id: str
    cluster_name: str
    current_auto_terminate_minutes: int | None
    recommended_auto_terminate_minutes: int
    avg_idle_time_per_day_minutes: float
    peak_usage_hours: list[int] = Field(default_factory=list)
    reason: str


class OptimizationSummary(BaseModel):
    """Summary of all optimization opportunities."""
    total_clusters_analyzed: int
    oversized_clusters: int
    underutilized_clusters: int
    total_potential_monthly_savings: float
    recommendations_count: int
    last_analysis_time: datetime


class MetricsCollectionResponse(BaseModel):
    """Response from metrics collection endpoint."""
    success: bool
    message: str
    clusters_processed: int
    metrics_persisted: bool
