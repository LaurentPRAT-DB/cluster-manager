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
    policy_id: str | None = None


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


# --- Spark Configuration Optimization Models ---


class SparkConfigImpact(str, Enum):
    """Impact category for Spark configuration recommendations."""
    PERFORMANCE = "performance"
    COST = "cost"
    RELIABILITY = "reliability"
    MEMORY = "memory"


class SparkConfigSeverity(str, Enum):
    """Severity level for configuration issues."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class SparkConfigRecommendation(BaseModel):
    """Recommendation for Spark configuration optimization."""
    cluster_id: str
    cluster_name: str
    setting: str
    current_value: str | None
    recommended_value: str
    impact: SparkConfigImpact
    severity: SparkConfigSeverity
    reason: str
    documentation_link: str | None = None


class ClusterSparkConfigAnalysis(BaseModel):
    """Full Spark configuration analysis for a cluster."""
    cluster_id: str
    cluster_name: str
    spark_version: str | None
    is_photon_enabled: bool
    aqe_enabled: bool | None
    total_issues: int
    recommendations: list[SparkConfigRecommendation]


# --- Cost Optimization Models ---


class CostOptimizationCategory(str, Enum):
    """Category of cost optimization recommendation."""
    SPOT_INSTANCES = "spot_instances"
    NODE_TYPE = "node_type"
    STORAGE = "storage"
    AUTOSCALING = "autoscaling"
    SERVERLESS = "serverless"


class CostRecommendationSeverity(str, Enum):
    """Severity/impact level for cost recommendations."""
    HIGH = "high"      # >30% potential savings
    MEDIUM = "medium"  # 10-30% potential savings
    LOW = "low"        # <10% potential savings


class CostOptimizationRecommendation(BaseModel):
    """Individual cost optimization recommendation."""
    cluster_id: str
    cluster_name: str
    category: CostOptimizationCategory
    current_state: str
    recommendation: str
    estimated_savings_percent: float
    severity: CostRecommendationSeverity
    reason: str
    implementation_steps: list[str] = []


class ClusterCostAnalysis(BaseModel):
    """Full cost optimization analysis for a cluster."""
    cluster_id: str
    cluster_name: str
    cloud_provider: str  # aws, azure, gcp
    node_type_id: str | None
    driver_node_type_id: str | None
    num_workers: int
    uses_spot_instances: bool
    spot_bid_price: float | None
    first_on_demand: int | None
    availability_zone: str | None
    ebs_volume_type: str | None
    total_recommendations: int
    total_potential_savings_percent: float
    recommendations: list[CostOptimizationRecommendation]


# --- Autoscaling Optimization Models ---


class AutoscalingIssueType(str, Enum):
    """Type of autoscaling configuration issue."""
    WIDE_RANGE = "wide_range"           # max >> min, suggests uncertainty
    NARROW_RANGE = "narrow_range"       # max ≈ min, consider fixed size
    HIGH_MINIMUM = "high_minimum"       # min_workers too high for idle periods
    NO_AUTOSCALING = "no_autoscaling"   # Fixed size could benefit from autoscaling
    INEFFICIENT_RANGE = "inefficient_range"  # Range doesn't match usage patterns


class AutoscalingSeverity(str, Enum):
    """Severity level for autoscaling issues."""
    HIGH = "high"      # >40% potential savings
    MEDIUM = "medium"  # 15-40% potential savings
    LOW = "low"        # <15% potential savings


class AutoscalingRecommendation(BaseModel):
    """Individual autoscaling optimization recommendation."""
    cluster_id: str
    cluster_name: str
    issue_type: AutoscalingIssueType
    current_config: str
    recommendation: str
    estimated_savings_percent: float
    severity: AutoscalingSeverity
    reason: str
    implementation_steps: list[str] = []


class ClusterAutoscalingAnalysis(BaseModel):
    """Full autoscaling analysis for a cluster."""
    cluster_id: str
    cluster_name: str
    cluster_type: ClusterType
    has_autoscaling: bool
    min_workers: int | None
    max_workers: int | None
    current_workers: int
    autoscale_range: int | None          # max - min
    range_ratio: float | None            # max / min
    auto_terminate_minutes: int | None
    total_issues: int
    total_potential_savings_percent: float
    recommendations: list[AutoscalingRecommendation]


# --- Node Type Right-Sizing Models ---


class NodeTypeCategory(str, Enum):
    """Category of node type based on resource profile."""
    MEMORY_OPTIMIZED = "memory_optimized"     # r5, r6i, E-series
    COMPUTE_OPTIMIZED = "compute_optimized"   # c5, c6i, F-series
    GENERAL_PURPOSE = "general_purpose"       # m5, m6i, D-series
    GPU = "gpu"                               # p3, p4, g4, NC-series
    STORAGE_OPTIMIZED = "storage_optimized"   # i3, d2, L-series
    UNKNOWN = "unknown"


class NodeTypeIssueType(str, Enum):
    """Type of node type configuration issue."""
    OVERSIZED_DRIVER = "oversized_driver"           # Driver larger than workers
    UNDERSIZED_DRIVER = "undersized_driver"         # Driver smaller than needed
    WRONG_CATEGORY = "wrong_category"               # Wrong instance family for workload
    OVERPROVISIONED = "overprovisioned"             # Instance too large for workload
    MISMATCHED_DRIVER_WORKER = "mismatched_driver_worker"  # Different families
    GPU_UNDERUTILIZED = "gpu_underutilized"         # GPU for non-ML workload
    LEGACY_INSTANCE = "legacy_instance"             # Old generation instance


class NodeTypeSeverity(str, Enum):
    """Severity level for node type issues."""
    HIGH = "high"      # >30% potential savings or performance impact
    MEDIUM = "medium"  # 15-30% potential savings
    LOW = "low"        # <15% potential savings


class NodeTypeRecommendation(BaseModel):
    """Individual node type optimization recommendation."""
    cluster_id: str
    cluster_name: str
    issue_type: NodeTypeIssueType
    current_config: str
    recommended_config: str
    estimated_savings_percent: float
    severity: NodeTypeSeverity
    reason: str
    implementation_steps: list[str] = []


class NodeTypeSpec(BaseModel):
    """Parsed node type specification."""
    instance_type: str
    category: NodeTypeCategory
    vcpus: int | None = None
    memory_gb: float | None = None
    gpu_count: int | None = None
    generation: str | None = None  # e.g., "5", "6", "6i"
    size: str | None = None        # e.g., "xlarge", "2xlarge"


class ClusterNodeTypeAnalysis(BaseModel):
    """Full node type analysis for a cluster."""
    cluster_id: str
    cluster_name: str
    cluster_type: ClusterType
    cloud_provider: str
    worker_node_type: str | None
    worker_node_category: NodeTypeCategory
    worker_spec: NodeTypeSpec | None
    driver_node_type: str | None
    driver_node_category: NodeTypeCategory
    driver_spec: NodeTypeSpec | None
    num_workers: int
    uses_same_driver_worker: bool
    total_issues: int
    total_potential_savings_percent: float
    recommendations: list[NodeTypeRecommendation]
