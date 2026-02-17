import { useMemo, useState } from "react";
import { createFileRoute, Link } from "@tanstack/react-router";
import {
  AlertTriangle,
  ArrowRight,
  ArrowUpDown,
  BarChart3,
  Calendar,
  CheckCircle,
  ChevronDown,
  ChevronUp,
  Clock,
  Cloud,
  Cpu,
  DollarSign,
  ExternalLink,
  HardDrive,
  LayoutGrid,
  Lightbulb,
  List,
  Loader2,
  Play,
  RefreshCw,
  Server,
  Settings,
  Target,
  TrendingDown,
  TrendingUp,
  Users,
  X,
  Zap,
} from "lucide-react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { toast } from "sonner";

import {
  useAutoscalingRecommendations,
  useCollectMetrics,
  useCostRecommendations,
  useJobRecommendations,
  useNodeTypeRecommendations,
  useOptimizationSummary,
  useOversizedClusters,
  useScheduleRecommendations,
  useSparkConfigRecommendations,
  useUtilizationTrends,
  type AutoscalingIssueType,
  type AutoscalingSeverity,
  type ClusterAutoscalingAnalysis,
  type ClusterCostAnalysis,
  type ClusterNodeTypeAnalysis,
  type ClusterSparkConfigAnalysis,
  type CostOptimizationCategory,
  type CostRecommendationSeverity,
  type NodeTypeCategory,
  type NodeTypeIssueType,
  type NodeTypeSeverity,
  type SparkConfigSeverity,
} from "@/lib/api";
import { cn, formatCurrency, formatDate, formatNumber } from "@/lib/utils";
import { ClusterActionsDropdown } from "@/components/clusters/cluster-actions-dropdown";

function MetricCard({
  title,
  value,
  icon: Icon,
  subtitle,
  variant = "default",
}: {
  title: string;
  value: string | number;
  icon: React.ElementType;
  subtitle?: string;
  variant?: "default" | "warning" | "success" | "danger";
}) {
  const variantStyles = {
    default: "bg-primary/10 text-primary",
    warning: "bg-yellow-100 dark:bg-yellow-900/30 text-yellow-600",
    success: "bg-green-100 dark:bg-green-900/30 text-green-600",
    danger: "bg-red-100 dark:bg-red-900/30 text-red-600",
  };

  return (
    <div className="bg-card rounded-lg border p-5">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm text-muted-foreground">{title}</p>
          <p className="text-3xl font-bold mt-1">{value}</p>
          {subtitle && (
            <p className="text-sm text-muted-foreground mt-1">{subtitle}</p>
          )}
        </div>
        <div className={cn("p-3 rounded-lg", variantStyles[variant])}>
          <Icon className="h-6 w-6" />
        </div>
      </div>
    </div>
  );
}

function EfficiencyBadge({ score }: { score: number }) {
  let color = "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400";
  if (score < 30) {
    color = "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400";
  } else if (score < 50) {
    color = "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400";
  }

  return (
    <span className={cn("inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium", color)}>
      {formatNumber(score, 0)}%
    </span>
  );
}

function ClusterTypeBadge({ type }: { type: string }) {
  const typeColors: Record<string, string> = {
    JOB: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400",
    INTERACTIVE: "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400",
    SQL: "bg-cyan-100 text-cyan-700 dark:bg-cyan-900/30 dark:text-cyan-400",
    PIPELINE: "bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400",
    MODELS: "bg-pink-100 text-pink-700 dark:bg-pink-900/30 dark:text-pink-400",
  };

  return (
    <span className={cn("inline-flex items-center px-2 py-0.5 rounded text-xs font-medium", typeColors[type] || "bg-gray-100 text-gray-700")}>
      {type}
    </span>
  );
}

function SeverityBadge({ severity }: { severity: SparkConfigSeverity }) {
  const severityColors: Record<SparkConfigSeverity, string> = {
    high: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400",
    medium: "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400",
    low: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400",
  };

  return (
    <span className={cn("inline-flex items-center px-2 py-0.5 rounded text-xs font-medium uppercase", severityColors[severity])}>
      {severity}
    </span>
  );
}

function PhotonBadge({ enabled }: { enabled: boolean }) {
  return enabled ? (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400">
      <Zap size={12} />
      Photon
    </span>
  ) : (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400">
      Standard
    </span>
  );
}

function AQEBadge({ enabled }: { enabled: boolean | null }) {
  if (enabled === null) {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400">
        AQE: Default
      </span>
    );
  }
  return enabled ? (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400">
      <CheckCircle size={12} />
      AQE
    </span>
  ) : (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
      <AlertTriangle size={12} />
      AQE Off
    </span>
  );
}

function SpotBadge({ usesSpot }: { usesSpot: boolean }) {
  return usesSpot ? (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400">
      <Zap size={12} />
      Spot
    </span>
  ) : (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400">
      On-Demand
    </span>
  );
}

function CloudBadge({ provider }: { provider: string }) {
  const colors: Record<string, string> = {
    aws: "bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400",
    azure: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400",
    gcp: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400",
  };
  return (
    <span className={cn("inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium uppercase", colors[provider] || "bg-gray-100 text-gray-600")}>
      <Cloud size={12} />
      {provider}
    </span>
  );
}

function CostSeverityBadge({ severity }: { severity: CostRecommendationSeverity }) {
  const severityColors: Record<CostRecommendationSeverity, string> = {
    high: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400",
    medium: "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400",
    low: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400",
  };

  return (
    <span className={cn("inline-flex items-center px-2 py-0.5 rounded text-xs font-medium uppercase", severityColors[severity])}>
      {severity}
    </span>
  );
}

function CostCategoryIcon({ category }: { category: CostOptimizationCategory }) {
  const icons: Record<CostOptimizationCategory, React.ReactNode> = {
    spot_instances: <Zap size={14} />,
    node_type: <Server size={14} />,
    storage: <HardDrive size={14} />,
    autoscaling: <TrendingUp size={14} />,
    serverless: <Cloud size={14} />,
  };
  return icons[category] || <DollarSign size={14} />;
}

function CostCategoryLabel({ category }: { category: CostOptimizationCategory }) {
  const labels: Record<CostOptimizationCategory, string> = {
    spot_instances: "Spot Instances",
    node_type: "Node Type",
    storage: "Storage",
    autoscaling: "Autoscaling",
    serverless: "Serverless",
  };
  return <span>{labels[category] || category}</span>;
}

function AutoscalingSeverityBadge({ severity }: { severity: AutoscalingSeverity }) {
  const severityColors: Record<AutoscalingSeverity, string> = {
    high: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400",
    medium: "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400",
    low: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400",
  };

  return (
    <span className={cn("inline-flex items-center px-2 py-0.5 rounded text-xs font-medium uppercase", severityColors[severity])}>
      {severity}
    </span>
  );
}

function AutoscalingIssueIcon({ issueType }: { issueType: AutoscalingIssueType }) {
  const icons: Record<AutoscalingIssueType, React.ReactNode> = {
    wide_range: <TrendingUp size={14} />,
    narrow_range: <TrendingDown size={14} />,
    high_minimum: <AlertTriangle size={14} />,
    no_autoscaling: <Target size={14} />,
    inefficient_range: <Settings size={14} />,
  };
  return icons[issueType] || <TrendingUp size={14} />;
}

function AutoscalingIssueLabel({ issueType }: { issueType: AutoscalingIssueType }) {
  const labels: Record<AutoscalingIssueType, string> = {
    wide_range: "Wide Range",
    narrow_range: "Narrow Range",
    high_minimum: "High Minimum",
    no_autoscaling: "No Autoscaling",
    inefficient_range: "Inefficient Config",
  };
  return <span>{labels[issueType] || issueType}</span>;
}

function AutoscalingBadge({ hasAutoscaling }: { hasAutoscaling: boolean }) {
  return hasAutoscaling ? (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400">
      <TrendingUp size={12} />
      Autoscale
    </span>
  ) : (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400">
      Fixed Size
    </span>
  );
}

function AutoTerminateBadge({ minutes }: { minutes: number | null }) {
  if (minutes === null || minutes === 0) {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
        <Clock size={12} />
        No Auto-Term
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400">
      <Clock size={12} />
      {minutes}m
    </span>
  );
}

// View Toggle Component
type ViewMode = "cards" | "list";

function ViewToggle({ view, onViewChange }: { view: ViewMode; onViewChange: (view: ViewMode) => void }) {
  return (
    <div className="flex items-center gap-1 bg-muted rounded-lg p-1">
      <button
        onClick={() => onViewChange("cards")}
        className={cn(
          "p-1.5 rounded transition-colors",
          view === "cards"
            ? "bg-background text-foreground shadow-sm"
            : "text-muted-foreground hover:text-foreground"
        )}
        title="Card view"
      >
        <LayoutGrid size={16} />
      </button>
      <button
        onClick={() => onViewChange("list")}
        className={cn(
          "p-1.5 rounded transition-colors",
          view === "list"
            ? "bg-background text-foreground shadow-sm"
            : "text-muted-foreground hover:text-foreground"
        )}
        title="List view"
      >
        <List size={16} />
      </button>
    </div>
  );
}

// Autoscaling Analysis Cluster Card Component
function AutoscalingAnalysisClusterCard({ cluster }: { cluster: ClusterAutoscalingAnalysis }) {
  const [expanded, setExpanded] = useState(false);

  const highSeverityCount = cluster.recommendations.filter((r) => r.severity === "high").length;
  const mediumSeverityCount = cluster.recommendations.filter((r) => r.severity === "medium").length;

  return (
    <div className="bg-muted/30 rounded-lg border">
      <div
        className="p-4 cursor-pointer hover:bg-muted/50 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
              <TrendingUp className="h-5 w-5 text-purple-600" />
            </div>
            <div>
              <Link
                to="/clusters/$clusterId"
                params={{ clusterId: cluster.cluster_id }}
                className="font-medium hover:text-primary"
                onClick={(e) => e.stopPropagation()}
              >
                {cluster.cluster_name}
              </Link>
              <div className="flex items-center gap-2 mt-1">
                <ClusterTypeBadge type={cluster.cluster_type} />
                <AutoscalingBadge hasAutoscaling={cluster.has_autoscaling} />
                <AutoTerminateBadge minutes={cluster.auto_terminate_minutes} />
                {cluster.has_autoscaling && cluster.min_workers !== null && cluster.max_workers !== null && (
                  <span className="text-xs text-muted-foreground">
                    {cluster.min_workers}-{cluster.max_workers} workers
                  </span>
                )}
                {!cluster.has_autoscaling && (
                  <span className="text-xs text-muted-foreground">
                    {cluster.current_workers} workers
                  </span>
                )}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-right">
              <div className="flex items-center gap-2">
                {highSeverityCount > 0 && (
                  <span className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
                    {highSeverityCount} high
                  </span>
                )}
                {mediumSeverityCount > 0 && (
                  <span className="text-xs px-2 py-0.5 rounded bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400">
                    {mediumSeverityCount} medium
                  </span>
                )}
              </div>
              <span className="text-sm font-medium text-green-600">
                Up to {cluster.total_potential_savings_percent}% savings
              </span>
            </div>
            {expanded ? (
              <ChevronUp size={18} className="text-muted-foreground" />
            ) : (
              <ChevronDown size={18} className="text-muted-foreground" />
            )}
          </div>
        </div>
      </div>

      {expanded && cluster.recommendations.length > 0 && (
        <div className="border-t p-4 space-y-3">
          {cluster.recommendations.map((rec, idx) => (
            <div
              key={idx}
              className="p-3 bg-background rounded-lg border"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="flex items-center gap-1 text-sm font-medium">
                      <AutoscalingIssueIcon issueType={rec.issue_type} />
                      <AutoscalingIssueLabel issueType={rec.issue_type} />
                    </span>
                    <AutoscalingSeverityBadge severity={rec.severity} />
                    <span className="text-xs text-green-600 font-medium">
                      ~{rec.estimated_savings_percent}% savings
                    </span>
                  </div>
                  <p className="text-sm font-medium mt-2">{rec.recommendation}</p>
                  <p className="text-sm text-muted-foreground mt-1">{rec.reason}</p>
                  <div className="flex items-center gap-4 mt-2 text-sm">
                    <span className="text-muted-foreground">
                      Current: <code className="font-mono text-xs bg-muted px-1 rounded">{rec.current_config}</code>
                    </span>
                  </div>
                  {rec.implementation_steps.length > 0 && (
                    <div className="mt-3 p-2 bg-muted/50 rounded text-sm">
                      <p className="font-medium text-xs text-muted-foreground mb-1">Implementation Steps:</p>
                      <ol className="list-decimal list-inside space-y-0.5 text-muted-foreground">
                        {rec.implementation_steps.map((step, i) => (
                          <li key={i}>{step}</li>
                        ))}
                      </ol>
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// Node Type Badge Components
function NodeTypeSeverityBadge({ severity }: { severity: NodeTypeSeverity }) {
  const severityColors: Record<NodeTypeSeverity, string> = {
    high: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400",
    medium: "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400",
    low: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400",
  };

  return (
    <span className={cn("inline-flex items-center px-2 py-0.5 rounded text-xs font-medium uppercase", severityColors[severity])}>
      {severity}
    </span>
  );
}

function NodeTypeIssueIcon({ issueType }: { issueType: NodeTypeIssueType }) {
  const icons: Record<NodeTypeIssueType, React.ReactNode> = {
    oversized_driver: <Server size={14} />,
    undersized_driver: <Server size={14} />,
    wrong_category: <Settings size={14} />,
    overprovisioned: <TrendingUp size={14} />,
    mismatched_driver_worker: <ArrowUpDown size={14} />,
    gpu_underutilized: <Cpu size={14} />,
    legacy_instance: <Clock size={14} />,
  };
  return icons[issueType] || <Server size={14} />;
}

function NodeTypeIssueLabel({ issueType }: { issueType: NodeTypeIssueType }) {
  const labels: Record<NodeTypeIssueType, string> = {
    oversized_driver: "Oversized Driver",
    undersized_driver: "Undersized Driver",
    wrong_category: "Wrong Category",
    overprovisioned: "Overprovisioned",
    mismatched_driver_worker: "Mismatched",
    gpu_underutilized: "GPU Underutilized",
    legacy_instance: "Legacy Instance",
  };
  return <span>{labels[issueType] || issueType}</span>;
}

function NodeTypeCategoryBadge({ category }: { category: NodeTypeCategory }) {
  const categoryColors: Record<NodeTypeCategory, string> = {
    memory_optimized: "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400",
    compute_optimized: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400",
    general_purpose: "bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-400",
    gpu: "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400",
    storage_optimized: "bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400",
    unknown: "bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-500",
  };
  const categoryLabels: Record<NodeTypeCategory, string> = {
    memory_optimized: "Memory",
    compute_optimized: "Compute",
    general_purpose: "General",
    gpu: "GPU",
    storage_optimized: "Storage",
    unknown: "Unknown",
  };
  return (
    <span className={cn("inline-flex items-center px-2 py-0.5 rounded text-xs font-medium", categoryColors[category])}>
      {categoryLabels[category]}
    </span>
  );
}

// Node Type Analysis Cluster Card Component
function NodeTypeAnalysisClusterCard({ cluster }: { cluster: ClusterNodeTypeAnalysis }) {
  const [expanded, setExpanded] = useState(false);

  const highSeverityCount = cluster.recommendations.filter((r) => r.severity === "high").length;
  const mediumSeverityCount = cluster.recommendations.filter((r) => r.severity === "medium").length;

  return (
    <div className="bg-muted/30 rounded-lg border">
      <div
        className="p-4 cursor-pointer hover:bg-muted/50 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
              <Server className="h-5 w-5 text-orange-600" />
            </div>
            <div>
              <Link
                to="/clusters/$clusterId"
                params={{ clusterId: cluster.cluster_id }}
                className="font-medium hover:text-primary"
                onClick={(e) => e.stopPropagation()}
              >
                {cluster.cluster_name}
              </Link>
              <div className="flex items-center gap-2 mt-1">
                <ClusterTypeBadge type={cluster.cluster_type} />
                <CloudBadge provider={cluster.cloud_provider} />
                <NodeTypeCategoryBadge category={cluster.worker_node_category} />
                {cluster.worker_node_type && (
                  <span className="text-xs text-muted-foreground">
                    {cluster.worker_node_type}
                  </span>
                )}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-right">
              <div className="flex items-center gap-2">
                {highSeverityCount > 0 && (
                  <span className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
                    {highSeverityCount} high
                  </span>
                )}
                {mediumSeverityCount > 0 && (
                  <span className="text-xs px-2 py-0.5 rounded bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400">
                    {mediumSeverityCount} medium
                  </span>
                )}
              </div>
              <span className="text-sm font-medium text-green-600">
                Up to {cluster.total_potential_savings_percent}% savings
              </span>
            </div>
            {expanded ? (
              <ChevronUp size={18} className="text-muted-foreground" />
            ) : (
              <ChevronDown size={18} className="text-muted-foreground" />
            )}
          </div>
        </div>
      </div>

      {expanded && cluster.recommendations.length > 0 && (
        <div className="border-t p-4 space-y-3">
          {/* Cluster Node Type Summary */}
          <div className="grid grid-cols-2 gap-4 p-3 bg-background rounded-lg border mb-3">
            <div>
              <p className="text-xs text-muted-foreground mb-1">Worker Nodes</p>
              <p className="text-sm font-medium">{cluster.worker_node_type || "Not specified"}</p>
              <div className="flex items-center gap-1 mt-1">
                <NodeTypeCategoryBadge category={cluster.worker_node_category} />
                {cluster.worker_spec?.vcpus && (
                  <span className="text-xs text-muted-foreground">{cluster.worker_spec.vcpus} vCPUs</span>
                )}
              </div>
            </div>
            <div>
              <p className="text-xs text-muted-foreground mb-1">Driver Node</p>
              <p className="text-sm font-medium">{cluster.driver_node_type || "Same as worker"}</p>
              <div className="flex items-center gap-1 mt-1">
                <NodeTypeCategoryBadge category={cluster.driver_node_category} />
                {cluster.driver_spec?.vcpus && (
                  <span className="text-xs text-muted-foreground">{cluster.driver_spec.vcpus} vCPUs</span>
                )}
              </div>
            </div>
          </div>

          {cluster.recommendations.map((rec, idx) => (
            <div
              key={idx}
              className="p-3 bg-background rounded-lg border"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="flex items-center gap-1 text-sm font-medium">
                      <NodeTypeIssueIcon issueType={rec.issue_type} />
                      <NodeTypeIssueLabel issueType={rec.issue_type} />
                    </span>
                    <NodeTypeSeverityBadge severity={rec.severity} />
                    <span className="text-xs text-green-600 font-medium">
                      ~{rec.estimated_savings_percent}% savings
                    </span>
                  </div>
                  <p className="text-sm font-medium mt-2">{rec.recommended_config}</p>
                  <p className="text-sm text-muted-foreground mt-1">{rec.reason}</p>
                  <div className="flex items-center gap-4 mt-2 text-sm">
                    <span className="text-muted-foreground">
                      Current: <code className="font-mono text-xs bg-muted px-1 rounded">{rec.current_config}</code>
                    </span>
                  </div>
                  {rec.implementation_steps.length > 0 && (
                    <div className="mt-3 p-2 bg-muted/50 rounded text-sm">
                      <p className="font-medium text-xs text-muted-foreground mb-1">Implementation Steps:</p>
                      <ol className="list-decimal list-inside space-y-0.5 text-muted-foreground">
                        {rec.implementation_steps.map((step, i) => (
                          <li key={i}>{step}</li>
                        ))}
                      </ol>
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// Cost Analysis Cluster Card Component
function CostAnalysisClusterCard({ cluster }: { cluster: ClusterCostAnalysis }) {
  const [expanded, setExpanded] = useState(false);

  const highSeverityCount = cluster.recommendations.filter((r) => r.severity === "high").length;
  const mediumSeverityCount = cluster.recommendations.filter((r) => r.severity === "medium").length;

  return (
    <div className="bg-muted/30 rounded-lg border">
      <div
        className="p-4 cursor-pointer hover:bg-muted/50 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
              <DollarSign className="h-5 w-5 text-green-600" />
            </div>
            <div>
              <Link
                to="/clusters/$clusterId"
                params={{ clusterId: cluster.cluster_id }}
                className="font-medium hover:text-primary"
                onClick={(e) => e.stopPropagation()}
              >
                {cluster.cluster_name}
              </Link>
              <div className="flex items-center gap-2 mt-1">
                <CloudBadge provider={cluster.cloud_provider} />
                <SpotBadge usesSpot={cluster.uses_spot_instances} />
                {cluster.node_type_id && (
                  <span className="text-xs text-muted-foreground">
                    {cluster.node_type_id}
                  </span>
                )}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-right">
              <div className="flex items-center gap-2">
                {highSeverityCount > 0 && (
                  <span className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
                    {highSeverityCount} high
                  </span>
                )}
                {mediumSeverityCount > 0 && (
                  <span className="text-xs px-2 py-0.5 rounded bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400">
                    {mediumSeverityCount} medium
                  </span>
                )}
              </div>
              <span className="text-sm font-medium text-green-600">
                Up to {cluster.total_potential_savings_percent}% savings
              </span>
            </div>
            {expanded ? (
              <ChevronUp size={18} className="text-muted-foreground" />
            ) : (
              <ChevronDown size={18} className="text-muted-foreground" />
            )}
          </div>
        </div>
      </div>

      {expanded && cluster.recommendations.length > 0 && (
        <div className="border-t p-4 space-y-3">
          {cluster.recommendations.map((rec, idx) => (
            <div
              key={idx}
              className="p-3 bg-background rounded-lg border"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="flex items-center gap-1 text-sm font-medium">
                      <CostCategoryIcon category={rec.category} />
                      <CostCategoryLabel category={rec.category} />
                    </span>
                    <CostSeverityBadge severity={rec.severity} />
                    <span className="text-xs text-green-600 font-medium">
                      ~{rec.estimated_savings_percent}% savings
                    </span>
                  </div>
                  <p className="text-sm font-medium mt-2">{rec.recommendation}</p>
                  <p className="text-sm text-muted-foreground mt-1">{rec.reason}</p>
                  <div className="flex items-center gap-4 mt-2 text-sm">
                    <span className="text-muted-foreground">
                      Current: <code className="font-mono text-xs bg-muted px-1 rounded">{rec.current_state}</code>
                    </span>
                  </div>
                  {rec.implementation_steps.length > 0 && (
                    <div className="mt-3 p-2 bg-muted/50 rounded text-sm">
                      <p className="font-medium text-xs text-muted-foreground mb-1">Implementation Steps:</p>
                      <ol className="list-decimal list-inside space-y-0.5 text-muted-foreground">
                        {rec.implementation_steps.map((step, i) => (
                          <li key={i}>{step}</li>
                        ))}
                      </ol>
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// Spark Config Cluster Card Component
function SparkConfigClusterCard({ cluster }: { cluster: ClusterSparkConfigAnalysis }) {
  const [expanded, setExpanded] = useState(false);

  const highSeverityCount = cluster.recommendations.filter((r) => r.severity === "high").length;
  const mediumSeverityCount = cluster.recommendations.filter((r) => r.severity === "medium").length;

  return (
    <div className="bg-muted/30 rounded-lg border">
      <div
        className="p-4 cursor-pointer hover:bg-muted/50 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg">
              <Cpu className="h-5 w-5 text-indigo-600" />
            </div>
            <div>
              <Link
                to="/clusters/$clusterId"
                params={{ clusterId: cluster.cluster_id }}
                className="font-medium hover:text-primary"
                onClick={(e) => e.stopPropagation()}
              >
                {cluster.cluster_name}
              </Link>
              <div className="flex items-center gap-2 mt-1">
                <PhotonBadge enabled={cluster.is_photon_enabled} />
                <AQEBadge enabled={cluster.aqe_enabled} />
                {cluster.spark_version && (
                  <span className="text-xs text-muted-foreground">
                    {cluster.spark_version}
                  </span>
                )}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-right">
              <div className="flex items-center gap-2">
                {highSeverityCount > 0 && (
                  <span className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
                    {highSeverityCount} high
                  </span>
                )}
                {mediumSeverityCount > 0 && (
                  <span className="text-xs px-2 py-0.5 rounded bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400">
                    {mediumSeverityCount} medium
                  </span>
                )}
                <span className="text-sm text-muted-foreground">
                  {cluster.total_issues} issue{cluster.total_issues !== 1 ? "s" : ""}
                </span>
              </div>
            </div>
            {expanded ? (
              <ChevronUp size={18} className="text-muted-foreground" />
            ) : (
              <ChevronDown size={18} className="text-muted-foreground" />
            )}
          </div>
        </div>
      </div>

      {expanded && cluster.recommendations.length > 0 && (
        <div className="border-t p-4 space-y-3">
          {cluster.recommendations.map((rec, idx) => (
            <div
              key={idx}
              className="p-3 bg-background rounded-lg border"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <code className="text-sm font-mono bg-muted px-1.5 py-0.5 rounded">
                      {rec.setting}
                    </code>
                    <SeverityBadge severity={rec.severity} />
                    <span className="text-xs text-muted-foreground capitalize">
                      {rec.impact}
                    </span>
                  </div>
                  <p className="text-sm text-muted-foreground mt-2">{rec.reason}</p>
                  <div className="flex items-center gap-4 mt-2 text-sm">
                    <span className="text-muted-foreground">
                      Current: <code className="font-mono">{rec.current_value || "not set"}</code>
                    </span>
                    <ArrowRight size={14} className="text-muted-foreground" />
                    <span className="text-green-600 dark:text-green-400">
                      Recommended: <code className="font-mono">{rec.recommended_value}</code>
                    </span>
                  </div>
                </div>
                {rec.documentation_link && (
                  <a
                    href={rec.documentation_link}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="p-2 hover:bg-muted rounded-lg transition-colors flex-shrink-0"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <ExternalLink size={16} className="text-muted-foreground" />
                  </a>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// List Row Components for alternative view

// Autoscaling List Row
function AutoscalingAnalysisListRow({ cluster }: { cluster: ClusterAutoscalingAnalysis }) {
  const highCount = cluster.recommendations.filter((r) => r.severity === "high").length;
  const mediumCount = cluster.recommendations.filter((r) => r.severity === "medium").length;

  return (
    <tr className="border-b hover:bg-muted/50 transition-colors">
      <td className="py-3 px-4">
        <Link
          to="/clusters/$clusterId"
          params={{ clusterId: cluster.cluster_id }}
          className="font-medium hover:text-primary"
        >
          {cluster.cluster_name}
        </Link>
      </td>
      <td className="py-3 px-4">
        <ClusterTypeBadge type={cluster.cluster_type} />
      </td>
      <td className="py-3 px-4">
        <AutoscalingBadge hasAutoscaling={cluster.has_autoscaling} />
      </td>
      <td className="py-3 px-4 text-center">
        {cluster.has_autoscaling && cluster.min_workers !== null && cluster.max_workers !== null
          ? `${cluster.min_workers}-${cluster.max_workers}`
          : cluster.current_workers}
      </td>
      <td className="py-3 px-4">
        <AutoTerminateBadge minutes={cluster.auto_terminate_minutes} />
      </td>
      <td className="py-3 px-4 text-center">
        <div className="flex items-center justify-center gap-1">
          {highCount > 0 && (
            <span className="text-xs px-1.5 py-0.5 rounded bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
              {highCount}
            </span>
          )}
          {mediumCount > 0 && (
            <span className="text-xs px-1.5 py-0.5 rounded bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400">
              {mediumCount}
            </span>
          )}
          {highCount === 0 && mediumCount === 0 && (
            <span className="text-muted-foreground">{cluster.total_issues}</span>
          )}
        </div>
      </td>
      <td className="py-3 px-4 text-right">
        <span className="text-green-600 font-medium">{cluster.total_potential_savings_percent}%</span>
      </td>
      <td className="py-3 px-4 text-right">
        <ClusterActionsDropdown clusterId={cluster.cluster_id} />
      </td>
    </tr>
  );
}

// Node Type List Row
function NodeTypeAnalysisListRow({ cluster }: { cluster: ClusterNodeTypeAnalysis }) {
  const highCount = cluster.recommendations.filter((r) => r.severity === "high").length;
  const mediumCount = cluster.recommendations.filter((r) => r.severity === "medium").length;

  return (
    <tr className="border-b hover:bg-muted/50 transition-colors">
      <td className="py-3 px-4">
        <Link
          to="/clusters/$clusterId"
          params={{ clusterId: cluster.cluster_id }}
          className="font-medium hover:text-primary"
        >
          {cluster.cluster_name}
        </Link>
      </td>
      <td className="py-3 px-4">
        <ClusterTypeBadge type={cluster.cluster_type} />
      </td>
      <td className="py-3 px-4">
        <CloudBadge provider={cluster.cloud_provider} />
      </td>
      <td className="py-3 px-4">
        <NodeTypeCategoryBadge category={cluster.worker_node_category} />
      </td>
      <td className="py-3 px-4 text-sm text-muted-foreground">
        {cluster.worker_node_type || "-"}
      </td>
      <td className="py-3 px-4 text-center">
        <div className="flex items-center justify-center gap-1">
          {highCount > 0 && (
            <span className="text-xs px-1.5 py-0.5 rounded bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
              {highCount}
            </span>
          )}
          {mediumCount > 0 && (
            <span className="text-xs px-1.5 py-0.5 rounded bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400">
              {mediumCount}
            </span>
          )}
          {highCount === 0 && mediumCount === 0 && (
            <span className="text-muted-foreground">{cluster.total_issues}</span>
          )}
        </div>
      </td>
      <td className="py-3 px-4 text-right">
        <span className="text-green-600 font-medium">{cluster.total_potential_savings_percent}%</span>
      </td>
      <td className="py-3 px-4 text-right">
        <ClusterActionsDropdown clusterId={cluster.cluster_id} />
      </td>
    </tr>
  );
}

// Cost Analysis List Row
function CostAnalysisListRow({ cluster }: { cluster: ClusterCostAnalysis }) {
  const highCount = cluster.recommendations.filter((r) => r.severity === "high").length;
  const mediumCount = cluster.recommendations.filter((r) => r.severity === "medium").length;

  return (
    <tr className="border-b hover:bg-muted/50 transition-colors">
      <td className="py-3 px-4">
        <Link
          to="/clusters/$clusterId"
          params={{ clusterId: cluster.cluster_id }}
          className="font-medium hover:text-primary"
        >
          {cluster.cluster_name}
        </Link>
      </td>
      <td className="py-3 px-4">
        <CloudBadge provider={cluster.cloud_provider} />
      </td>
      <td className="py-3 px-4">
        <SpotBadge usesSpot={cluster.uses_spot_instances} />
      </td>
      <td className="py-3 px-4 text-sm text-muted-foreground">
        {cluster.node_type_id || "-"}
      </td>
      <td className="py-3 px-4 text-center">{cluster.num_workers}</td>
      <td className="py-3 px-4 text-center">
        <div className="flex items-center justify-center gap-1">
          {highCount > 0 && (
            <span className="text-xs px-1.5 py-0.5 rounded bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
              {highCount}
            </span>
          )}
          {mediumCount > 0 && (
            <span className="text-xs px-1.5 py-0.5 rounded bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400">
              {mediumCount}
            </span>
          )}
          {highCount === 0 && mediumCount === 0 && (
            <span className="text-muted-foreground">{cluster.total_recommendations}</span>
          )}
        </div>
      </td>
      <td className="py-3 px-4 text-right">
        <span className="text-green-600 font-medium">{cluster.total_potential_savings_percent}%</span>
      </td>
      <td className="py-3 px-4 text-right">
        <ClusterActionsDropdown clusterId={cluster.cluster_id} />
      </td>
    </tr>
  );
}

// Spark Config List Row
function SparkConfigListRow({ cluster }: { cluster: ClusterSparkConfigAnalysis }) {
  const highCount = cluster.recommendations.filter((r) => r.severity === "high").length;
  const mediumCount = cluster.recommendations.filter((r) => r.severity === "medium").length;

  return (
    <tr className="border-b hover:bg-muted/50 transition-colors">
      <td className="py-3 px-4">
        <Link
          to="/clusters/$clusterId"
          params={{ clusterId: cluster.cluster_id }}
          className="font-medium hover:text-primary"
        >
          {cluster.cluster_name}
        </Link>
      </td>
      <td className="py-3 px-4">
        <PhotonBadge enabled={cluster.is_photon_enabled} />
      </td>
      <td className="py-3 px-4">
        <AQEBadge enabled={cluster.aqe_enabled} />
      </td>
      <td className="py-3 px-4 text-sm text-muted-foreground">
        {cluster.spark_version || "-"}
      </td>
      <td className="py-3 px-4 text-center">
        <div className="flex items-center justify-center gap-1">
          {highCount > 0 && (
            <span className="text-xs px-1.5 py-0.5 rounded bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
              {highCount}
            </span>
          )}
          {mediumCount > 0 && (
            <span className="text-xs px-1.5 py-0.5 rounded bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400">
              {mediumCount}
            </span>
          )}
          {highCount === 0 && mediumCount === 0 && (
            <span className="text-muted-foreground">{cluster.total_issues}</span>
          )}
        </div>
      </td>
      <td className="py-3 px-4 text-right">
        <ClusterActionsDropdown clusterId={cluster.cluster_id} />
      </td>
    </tr>
  );
}

// Sorting types and helper
type SortField = "cluster_name" | "cluster_type" | "current_workers" | "avg_efficiency_score" | "recommended_workers" | "potential_cost_savings";
type SortDirection = "asc" | "desc";

// Sort field types for each tab
type SparkConfigSortField = "cluster_name" | "is_photon_enabled" | "aqe_enabled" | "spark_version" | "total_issues";
type CostSortField = "cluster_name" | "cloud_provider" | "uses_spot_instances" | "node_type_id" | "num_workers" | "total_recommendations" | "total_potential_savings_percent";
type AutoscalingSortField = "cluster_name" | "cluster_type" | "has_autoscaling" | "current_workers" | "auto_terminate_minutes" | "total_issues" | "total_potential_savings_percent";
type NodeTypeSortField = "cluster_name" | "cluster_type" | "cloud_provider" | "worker_node_category" | "worker_node_type" | "total_issues" | "total_potential_savings_percent";
type JobsSortField = "source_cluster_name" | "target_cluster_name" | "job_count" | "estimated_savings";
type ScheduleSortField = "cluster_name" | "current_auto_terminate_minutes" | "recommended_auto_terminate_minutes" | "avg_idle_time_per_day_minutes";

// Generic sortable header component
function GenericSortableHeader<T extends string>({
  label,
  field,
  currentSort,
  onSort,
  align = "left",
}: {
  label: string;
  field: T;
  currentSort: { field: T; direction: SortDirection };
  onSort: (field: T) => void;
  align?: "left" | "center" | "right";
}) {
  const isActive = currentSort.field === field;
  const textAlign = align === "center" ? "text-center" : align === "right" ? "text-right" : "text-left";

  return (
    <th
      className={cn(
        "py-3 px-4 font-medium text-sm cursor-pointer hover:bg-muted/70 transition-colors select-none",
        textAlign
      )}
      onClick={() => onSort(field)}
    >
      <div className={cn("flex items-center gap-1", align === "center" && "justify-center", align === "right" && "justify-end")}>
        {label}
        {isActive ? (
          currentSort.direction === "asc" ? (
            <ChevronUp size={14} className="text-primary" />
          ) : (
            <ChevronDown size={14} className="text-primary" />
          )
        ) : (
          <ArrowUpDown size={14} className="text-muted-foreground opacity-50" />
        )}
      </div>
    </th>
  );
}

function SortableHeader({
  label,
  field,
  currentSort,
  onSort,
  align = "left",
}: {
  label: string;
  field: SortField;
  currentSort: { field: SortField; direction: SortDirection };
  onSort: (field: SortField) => void;
  align?: "left" | "center" | "right";
}) {
  const isActive = currentSort.field === field;
  const alignClass = align === "center" ? "justify-center" : align === "right" ? "justify-end" : "justify-start";

  return (
    <th
      className={cn(
        "py-3 px-4 font-medium text-sm cursor-pointer hover:bg-muted/70 transition-colors select-none",
        align === "center" ? "text-center" : align === "right" ? "text-right" : "text-left"
      )}
      onClick={() => onSort(field)}
    >
      <div className={cn("flex items-center gap-1", alignClass)}>
        {label}
        {isActive ? (
          currentSort.direction === "asc" ? (
            <ChevronUp size={14} className="text-primary" />
          ) : (
            <ChevronDown size={14} className="text-primary" />
          )
        ) : (
          <ArrowUpDown size={14} className="text-muted-foreground opacity-50" />
        )}
      </div>
    </th>
  );
}

type TabType = "trends" | "oversized" | "spark-config" | "cost" | "autoscaling" | "node-types" | "jobs" | "schedule";

function OptimizationPage() {
  const [activeTab, setActiveTab] = useState<TabType>("trends");

  const { data: summary, isLoading: summaryLoading } = useOptimizationSummary();
  const { data: oversizedClusters, isLoading: oversizedLoading } = useOversizedClusters(5);
  const { data: jobRecommendations, isLoading: jobsLoading } = useJobRecommendations();
  const { data: scheduleRecommendations, isLoading: scheduleLoading } = useScheduleRecommendations();
  const { data: trendsData, isLoading: trendsLoading } = useUtilizationTrends(30, 7);
  const { data: sparkConfigData, isLoading: sparkConfigLoading } = useSparkConfigRecommendations();
  const { data: costData, isLoading: costLoading } = useCostRecommendations();
  const { data: autoscalingData, isLoading: autoscalingLoading } = useAutoscalingRecommendations();
  const { data: nodeTypeData, isLoading: nodeTypeLoading } = useNodeTypeRecommendations();

  // Sorting state for oversized clusters table
  const [oversizedSort, setOversizedSort] = useState<{ field: SortField; direction: SortDirection }>({
    field: "potential_cost_savings",
    direction: "desc",
  });

  // View mode state for each tab
  const [sparkConfigView, setSparkConfigView] = useState<ViewMode>("cards");
  const [costView, setCostView] = useState<ViewMode>("cards");
  const [autoscalingView, setAutoscalingView] = useState<ViewMode>("cards");
  const [nodeTypeView, setNodeTypeView] = useState<ViewMode>("cards");
  const [jobsView, setJobsView] = useState<ViewMode>("cards");
  const [scheduleView, setScheduleView] = useState<ViewMode>("cards");

  // Sort states for each list view
  const [sparkConfigSort, setSparkConfigSort] = useState<{ field: SparkConfigSortField; direction: SortDirection }>({
    field: "total_issues",
    direction: "desc",
  });
  const [costSort, setCostSort] = useState<{ field: CostSortField; direction: SortDirection }>({
    field: "total_potential_savings_percent",
    direction: "desc",
  });
  const [autoscalingSort, setAutoscalingSort] = useState<{ field: AutoscalingSortField; direction: SortDirection }>({
    field: "total_potential_savings_percent",
    direction: "desc",
  });
  const [nodeTypeSort, setNodeTypeSort] = useState<{ field: NodeTypeSortField; direction: SortDirection }>({
    field: "total_potential_savings_percent",
    direction: "desc",
  });
  const [jobsSort, setJobsSort] = useState<{ field: JobsSortField; direction: SortDirection }>({
    field: "job_count",
    direction: "desc",
  });
  const [scheduleSort, setScheduleSort] = useState<{ field: ScheduleSortField; direction: SortDirection }>({
    field: "avg_idle_time_per_day_minutes",
    direction: "desc",
  });

  // Filter states for each tab
  const [nodeTypeFilter, setNodeTypeFilter] = useState<{ type: "issue" | "category"; value: string } | null>(null);
  const [costFilter, setCostFilter] = useState<{ type: "category"; value: string } | null>(null);
  const [autoscalingFilter, setAutoscalingFilter] = useState<{ type: "issue"; value: string } | null>(null);
  const [sparkConfigFilter, setSparkConfigFilter] = useState<{ type: "impact" | "severity"; value: string } | null>(null);

  const handleOversizedSort = (field: SortField) => {
    setOversizedSort((prev) => ({
      field,
      direction: prev.field === field && prev.direction === "desc" ? "asc" : "desc",
    }));
  };

  // Sort oversized clusters
  const sortedOversizedClusters = useMemo(() => {
    if (!oversizedClusters) return [];
    return [...oversizedClusters].sort((a, b) => {
      const { field, direction } = oversizedSort;
      let comparison = 0;

      switch (field) {
        case "cluster_name":
          comparison = a.cluster_name.localeCompare(b.cluster_name);
          break;
        case "cluster_type":
          comparison = a.cluster_type.localeCompare(b.cluster_type);
          break;
        case "current_workers":
          comparison = a.current_workers - b.current_workers;
          break;
        case "avg_efficiency_score":
          comparison = a.avg_efficiency_score - b.avg_efficiency_score;
          break;
        case "recommended_workers":
          comparison = a.recommended_workers - b.recommended_workers;
          break;
        case "potential_cost_savings":
          comparison = a.potential_cost_savings - b.potential_cost_savings;
          break;
      }

      return direction === "asc" ? comparison : -comparison;
    });
  }, [oversizedClusters, oversizedSort]);

  // Sort handlers for each tab
  const handleSparkConfigSort = (field: SparkConfigSortField) => {
    setSparkConfigSort((prev) => ({
      field,
      direction: prev.field === field && prev.direction === "desc" ? "asc" : "desc",
    }));
  };

  const handleCostSort = (field: CostSortField) => {
    setCostSort((prev) => ({
      field,
      direction: prev.field === field && prev.direction === "desc" ? "asc" : "desc",
    }));
  };

  const handleAutoscalingSort = (field: AutoscalingSortField) => {
    setAutoscalingSort((prev) => ({
      field,
      direction: prev.field === field && prev.direction === "desc" ? "asc" : "desc",
    }));
  };

  const handleNodeTypeSort = (field: NodeTypeSortField) => {
    setNodeTypeSort((prev) => ({
      field,
      direction: prev.field === field && prev.direction === "desc" ? "asc" : "desc",
    }));
  };

  const handleJobsSort = (field: JobsSortField) => {
    setJobsSort((prev) => ({
      field,
      direction: prev.field === field && prev.direction === "desc" ? "asc" : "desc",
    }));
  };

  const handleScheduleSort = (field: ScheduleSortField) => {
    setScheduleSort((prev) => ({
      field,
      direction: prev.field === field && prev.direction === "desc" ? "asc" : "desc",
    }));
  };

  // Filtered and sorted data for Spark Config
  const sortedSparkConfigData = useMemo(() => {
    if (!sparkConfigData) return [];

    // Apply filter first
    let filtered = [...sparkConfigData];
    if (sparkConfigFilter) {
      if (sparkConfigFilter.type === "impact") {
        filtered = filtered.filter((c) =>
          c.recommendations.some((r) => r.impact === sparkConfigFilter.value)
        );
      } else if (sparkConfigFilter.type === "severity") {
        filtered = filtered.filter((c) =>
          c.recommendations.some((r) => r.severity === sparkConfigFilter.value)
        );
      }
    }

    // Then sort
    return filtered.sort((a, b) => {
      const { field, direction } = sparkConfigSort;
      let comparison = 0;
      switch (field) {
        case "cluster_name":
          comparison = a.cluster_name.localeCompare(b.cluster_name);
          break;
        case "is_photon_enabled":
          comparison = (a.is_photon_enabled ? 1 : 0) - (b.is_photon_enabled ? 1 : 0);
          break;
        case "aqe_enabled":
          comparison = (a.aqe_enabled === null ? 0 : a.aqe_enabled ? 1 : -1) - (b.aqe_enabled === null ? 0 : b.aqe_enabled ? 1 : -1);
          break;
        case "spark_version":
          comparison = (a.spark_version || "").localeCompare(b.spark_version || "");
          break;
        case "total_issues":
          comparison = a.total_issues - b.total_issues;
          break;
      }
      return direction === "asc" ? comparison : -comparison;
    });
  }, [sparkConfigData, sparkConfigSort, sparkConfigFilter]);

  // Filtered and sorted data for Cost
  const sortedCostData = useMemo(() => {
    if (!costData) return [];

    // Apply filter first
    let filtered = [...costData];
    if (costFilter) {
      if (costFilter.type === "category") {
        filtered = filtered.filter((c) =>
          c.recommendations.some((r) => r.category === costFilter.value)
        );
      }
    }

    // Then sort
    return filtered.sort((a, b) => {
      const { field, direction } = costSort;
      let comparison = 0;
      switch (field) {
        case "cluster_name":
          comparison = a.cluster_name.localeCompare(b.cluster_name);
          break;
        case "cloud_provider":
          comparison = a.cloud_provider.localeCompare(b.cloud_provider);
          break;
        case "uses_spot_instances":
          comparison = (a.uses_spot_instances ? 1 : 0) - (b.uses_spot_instances ? 1 : 0);
          break;
        case "node_type_id":
          comparison = (a.node_type_id || "").localeCompare(b.node_type_id || "");
          break;
        case "num_workers":
          comparison = a.num_workers - b.num_workers;
          break;
        case "total_recommendations":
          comparison = a.total_recommendations - b.total_recommendations;
          break;
        case "total_potential_savings_percent":
          comparison = a.total_potential_savings_percent - b.total_potential_savings_percent;
          break;
      }
      return direction === "asc" ? comparison : -comparison;
    });
  }, [costData, costSort, costFilter]);

  // Filtered and sorted data for Autoscaling
  const sortedAutoscalingData = useMemo(() => {
    if (!autoscalingData) return [];

    // Apply filter first
    let filtered = [...autoscalingData];
    if (autoscalingFilter) {
      if (autoscalingFilter.type === "issue") {
        filtered = filtered.filter((c) =>
          c.recommendations.some((r) => r.issue_type === autoscalingFilter.value)
        );
      }
    }

    // Then sort
    return filtered.sort((a, b) => {
      const { field, direction } = autoscalingSort;
      let comparison = 0;
      switch (field) {
        case "cluster_name":
          comparison = a.cluster_name.localeCompare(b.cluster_name);
          break;
        case "cluster_type":
          comparison = a.cluster_type.localeCompare(b.cluster_type);
          break;
        case "has_autoscaling":
          comparison = (a.has_autoscaling ? 1 : 0) - (b.has_autoscaling ? 1 : 0);
          break;
        case "current_workers":
          comparison = a.current_workers - b.current_workers;
          break;
        case "auto_terminate_minutes":
          comparison = (a.auto_terminate_minutes || 0) - (b.auto_terminate_minutes || 0);
          break;
        case "total_issues":
          comparison = a.total_issues - b.total_issues;
          break;
        case "total_potential_savings_percent":
          comparison = a.total_potential_savings_percent - b.total_potential_savings_percent;
          break;
      }
      return direction === "asc" ? comparison : -comparison;
    });
  }, [autoscalingData, autoscalingSort, autoscalingFilter]);

  // Filtered and sorted data for Node Type
  const sortedNodeTypeData = useMemo(() => {
    if (!nodeTypeData) return [];

    // Apply filter first
    let filtered = [...nodeTypeData];
    if (nodeTypeFilter) {
      if (nodeTypeFilter.type === "issue") {
        filtered = filtered.filter((c) =>
          c.recommendations.some((r) => r.issue_type === nodeTypeFilter.value)
        );
      } else if (nodeTypeFilter.type === "category") {
        filtered = filtered.filter((c) => c.worker_node_category === nodeTypeFilter.value);
      }
    }

    // Then sort
    return filtered.sort((a, b) => {
      const { field, direction } = nodeTypeSort;
      let comparison = 0;
      switch (field) {
        case "cluster_name":
          comparison = a.cluster_name.localeCompare(b.cluster_name);
          break;
        case "cluster_type":
          comparison = a.cluster_type.localeCompare(b.cluster_type);
          break;
        case "cloud_provider":
          comparison = a.cloud_provider.localeCompare(b.cloud_provider);
          break;
        case "worker_node_category":
          comparison = a.worker_node_category.localeCompare(b.worker_node_category);
          break;
        case "worker_node_type":
          comparison = (a.worker_node_type || "").localeCompare(b.worker_node_type || "");
          break;
        case "total_issues":
          comparison = a.total_issues - b.total_issues;
          break;
        case "total_potential_savings_percent":
          comparison = a.total_potential_savings_percent - b.total_potential_savings_percent;
          break;
      }
      return direction === "asc" ? comparison : -comparison;
    });
  }, [nodeTypeData, nodeTypeSort, nodeTypeFilter]);

  // Sorted data for Jobs
  const sortedJobRecommendations = useMemo(() => {
    if (!jobRecommendations) return [];
    return [...jobRecommendations].sort((a, b) => {
      const { field, direction } = jobsSort;
      let comparison = 0;
      switch (field) {
        case "source_cluster_name":
          comparison = a.source_cluster_name.localeCompare(b.source_cluster_name);
          break;
        case "target_cluster_name":
          comparison = a.target_cluster_name.localeCompare(b.target_cluster_name);
          break;
        case "job_count":
          comparison = a.job_count - b.job_count;
          break;
        case "estimated_savings":
          comparison = a.estimated_savings.localeCompare(b.estimated_savings);
          break;
      }
      return direction === "asc" ? comparison : -comparison;
    });
  }, [jobRecommendations, jobsSort]);

  // Sorted data for Schedule
  const sortedScheduleRecommendations = useMemo(() => {
    if (!scheduleRecommendations) return [];
    return [...scheduleRecommendations].sort((a, b) => {
      const { field, direction } = scheduleSort;
      let comparison = 0;
      switch (field) {
        case "cluster_name":
          comparison = a.cluster_name.localeCompare(b.cluster_name);
          break;
        case "current_auto_terminate_minutes":
          comparison = (a.current_auto_terminate_minutes || 0) - (b.current_auto_terminate_minutes || 0);
          break;
        case "recommended_auto_terminate_minutes":
          comparison = a.recommended_auto_terminate_minutes - b.recommended_auto_terminate_minutes;
          break;
        case "avg_idle_time_per_day_minutes":
          comparison = a.avg_idle_time_per_day_minutes - b.avg_idle_time_per_day_minutes;
          break;
      }
      return direction === "asc" ? comparison : -comparison;
    });
  }, [scheduleRecommendations, scheduleSort]);

  const collectMetrics = useCollectMetrics();

  const handleCollectMetrics = () => {
    collectMetrics.mutate(undefined, {
      onSuccess: (data) => {
        toast.success(data.message);
      },
      onError: (error) => {
        toast.error(`Failed to collect metrics: ${error.message}`);
      },
    });
  };

  const tabs = [
    { id: "trends" as const, label: "Utilization Trends", icon: TrendingUp },
    { id: "oversized" as const, label: "Oversized Clusters", icon: TrendingDown },
    { id: "spark-config" as const, label: "Spark Config", icon: Settings },
    { id: "cost" as const, label: "Cost Optimization", icon: DollarSign },
    { id: "autoscaling" as const, label: "Autoscaling", icon: ArrowUpDown },
    { id: "node-types" as const, label: "Node Types", icon: Server },
    { id: "jobs" as const, label: "Job Recommendations", icon: Play },
    { id: "schedule" as const, label: "Schedule Optimization", icon: Calendar },
  ];

  // Prepare chart data - reverse to show oldest first
  const chartData = trendsData?.trends
    ?.slice()
    .reverse()
    .map((d) => ({
      date: formatDate(d.date),
      efficiency: d.avg_efficiency,
      efficiencyMA: d.efficiency_moving_avg,
      dbu: d.total_dbu,
      dbuMA: d.dbu_moving_avg,
      oversized: d.oversized_count,
      underutilized: d.underutilized_count,
      clusters: d.total_clusters,
    })) || [];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Optimization</h1>
          <p className="text-muted-foreground">
            Identify cost-saving opportunities and optimize cluster utilization
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={handleCollectMetrics}
            disabled={collectMetrics.isPending}
            className="flex items-center gap-2 px-3 py-2 text-sm bg-primary text-primary-foreground hover:bg-primary/90 rounded-lg transition-colors disabled:opacity-50"
          >
            {collectMetrics.isPending ? (
              <Loader2 size={16} className="animate-spin" />
            ) : (
              <BarChart3 size={16} />
            )}
            Collect Metrics
          </button>
          <button
            onClick={() => window.location.reload()}
            className="flex items-center gap-2 px-3 py-2 text-sm bg-secondary hover:bg-secondary/80 rounded-lg transition-colors"
          >
            <RefreshCw size={16} />
            Refresh
          </button>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {summaryLoading ? (
          Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="bg-card rounded-lg border p-5 animate-pulse">
              <div className="h-4 bg-muted rounded w-24 mb-2" />
              <div className="h-8 bg-muted rounded w-32" />
            </div>
          ))
        ) : summary ? (
          <>
            <MetricCard
              title="Clusters Analyzed"
              value={summary.total_clusters_analyzed}
              icon={Target}
              subtitle="Total in workspace"
            />
            <MetricCard
              title="Oversized Clusters"
              value={summary.oversized_clusters}
              icon={AlertTriangle}
              subtitle=">= 20 workers"
              variant={summary.oversized_clusters > 0 ? "warning" : "default"}
            />
            <MetricCard
              title="Underutilized"
              value={summary.underutilized_clusters}
              icon={TrendingDown}
              subtitle=">= 10 workers"
              variant={summary.underutilized_clusters > 0 ? "warning" : "default"}
            />
            <MetricCard
              title="Potential Savings"
              value={formatCurrency(summary.total_potential_monthly_savings)}
              icon={DollarSign}
              subtitle="Per month"
              variant={summary.total_potential_monthly_savings > 100 ? "success" : "default"}
            />
          </>
        ) : null}
      </div>

      {/* Tabs */}
      <div className="border-b">
        <nav className="flex gap-4">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={cn(
                  "flex items-center gap-2 px-3 py-2 border-b-2 transition-colors",
                  activeTab === tab.id
                    ? "border-primary text-primary font-medium"
                    : "border-transparent text-muted-foreground hover:text-foreground"
                )}
              >
                <Icon size={18} />
                {tab.label}
              </button>
            );
          })}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="bg-card rounded-lg border">
        {activeTab === "trends" && (
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <TrendingUp className="h-5 w-5 text-blue-500" />
                <h2 className="text-lg font-semibold">Utilization Trends</h2>
              </div>
              {trendsData?.summary && (
                <div className="flex items-center gap-4 text-sm">
                  <span className="text-muted-foreground">
                    {trendsData.summary.data_points} days of data
                  </span>
                  {trendsData.summary.efficiency_trend && (
                    <span className={cn(
                      "px-2 py-1 rounded-full text-xs font-medium",
                      trendsData.summary.efficiency_trend === "improving"
                        ? "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400"
                        : "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400"
                    )}>
                      Efficiency {trendsData.summary.efficiency_trend}
                    </span>
                  )}
                </div>
              )}
            </div>
            <p className="text-sm text-muted-foreground mb-6">
              Historical cluster utilization metrics with 7-day moving averages. Click "Collect Metrics" daily to build trend data.
            </p>

            {trendsLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : chartData.length > 0 ? (
              <div className="space-y-8">
                {/* Efficiency Chart */}
                <div>
                  <h3 className="text-sm font-medium mb-3 flex items-center gap-2">
                    <BarChart3 size={16} />
                    Efficiency Score (%)
                  </h3>
                  <ResponsiveContainer width="100%" height={250}>
                    <AreaChart data={chartData}>
                      <defs>
                        <linearGradient id="efficiencyGradient" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.3} />
                          <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                      <XAxis
                        dataKey="date"
                        tick={{ fontSize: 11 }}
                        tickLine={false}
                        axisLine={false}
                      />
                      <YAxis
                        tick={{ fontSize: 11 }}
                        tickLine={false}
                        axisLine={false}
                        domain={[0, 100]}
                        tickFormatter={(v) => `${v}%`}
                      />
                      <Tooltip
                        content={({ active, payload }) => {
                          if (active && payload && payload.length) {
                            return (
                              <div className="bg-popover border rounded-lg p-3 shadow-lg">
                                <p className="font-medium">{payload[0].payload.date}</p>
                                <p className="text-sm text-primary">
                                  Efficiency: {formatNumber(payload[0].payload.efficiency, 1)}%
                                </p>
                                <p className="text-sm text-muted-foreground">
                                  7-day MA: {formatNumber(payload[0].payload.efficiencyMA, 1)}%
                                </p>
                              </div>
                            );
                          }
                          return null;
                        }}
                      />
                      <Area
                        type="monotone"
                        dataKey="efficiency"
                        stroke="hsl(var(--primary))"
                        fill="url(#efficiencyGradient)"
                        strokeWidth={2}
                      />
                      <Line
                        type="monotone"
                        dataKey="efficiencyMA"
                        stroke="hsl(var(--primary))"
                        strokeWidth={2}
                        strokeDasharray="5 5"
                        dot={false}
                        name="7-day MA"
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>

                {/* DBU Usage Chart */}
                <div>
                  <h3 className="text-sm font-medium mb-3 flex items-center gap-2">
                    <Zap size={16} />
                    Daily DBU Consumption
                  </h3>
                  <ResponsiveContainer width="100%" height={250}>
                    <BarChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                      <XAxis
                        dataKey="date"
                        tick={{ fontSize: 11 }}
                        tickLine={false}
                        axisLine={false}
                      />
                      <YAxis
                        tick={{ fontSize: 11 }}
                        tickLine={false}
                        axisLine={false}
                        tickFormatter={(v) => formatNumber(v, 0)}
                      />
                      <Tooltip
                        content={({ active, payload }) => {
                          if (active && payload && payload.length) {
                            return (
                              <div className="bg-popover border rounded-lg p-3 shadow-lg">
                                <p className="font-medium">{payload[0].payload.date}</p>
                                <p className="text-sm text-blue-600">
                                  DBU: {formatNumber(payload[0].payload.dbu, 1)}
                                </p>
                                <p className="text-sm text-muted-foreground">
                                  7-day MA: {formatNumber(payload[0].payload.dbuMA, 1)}
                                </p>
                                <p className="text-sm text-muted-foreground">
                                  Est. Cost: {formatCurrency(payload[0].payload.dbu * 0.15)}
                                </p>
                              </div>
                            );
                          }
                          return null;
                        }}
                      />
                      <Bar
                        dataKey="dbu"
                        fill="hsl(217, 91%, 60%)"
                        radius={[4, 4, 0, 0]}
                        name="Daily DBU"
                      />
                      <Line
                        type="monotone"
                        dataKey="dbuMA"
                        stroke="hsl(217, 91%, 40%)"
                        strokeWidth={2}
                        dot={false}
                        name="7-day MA"
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>

                {/* Cluster Health Chart */}
                <div>
                  <h3 className="text-sm font-medium mb-3 flex items-center gap-2">
                    <AlertTriangle size={16} />
                    Cluster Health Overview
                  </h3>
                  <ResponsiveContainer width="100%" height={200}>
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                      <XAxis
                        dataKey="date"
                        tick={{ fontSize: 11 }}
                        tickLine={false}
                        axisLine={false}
                      />
                      <YAxis
                        tick={{ fontSize: 11 }}
                        tickLine={false}
                        axisLine={false}
                      />
                      <Tooltip
                        content={({ active, payload }) => {
                          if (active && payload && payload.length) {
                            return (
                              <div className="bg-popover border rounded-lg p-3 shadow-lg">
                                <p className="font-medium">{payload[0].payload.date}</p>
                                <p className="text-sm">
                                  Total Clusters: {payload[0].payload.clusters}
                                </p>
                                <p className="text-sm text-red-600">
                                  Oversized: {payload[0].payload.oversized}
                                </p>
                                <p className="text-sm text-yellow-600">
                                  Underutilized: {payload[0].payload.underutilized}
                                </p>
                              </div>
                            );
                          }
                          return null;
                        }}
                      />
                      <Legend />
                      <Line
                        type="monotone"
                        dataKey="oversized"
                        stroke="hsl(0, 84%, 60%)"
                        strokeWidth={2}
                        name="Oversized"
                        dot={{ r: 3 }}
                      />
                      <Line
                        type="monotone"
                        dataKey="underutilized"
                        stroke="hsl(45, 93%, 47%)"
                        strokeWidth={2}
                        name="Underutilized"
                        dot={{ r: 3 }}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            ) : (
              <div className="text-center py-12 text-muted-foreground">
                <BarChart3 className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>No historical data available</p>
                <p className="text-sm mt-1">
                  Click "Collect Metrics" to start gathering utilization data for trend analysis
                </p>
              </div>
            )}
          </div>
        )}

        {activeTab === "oversized" && (
          <div className="p-6">
            <div className="flex items-center gap-2 mb-4">
              <TrendingDown className="h-5 w-5 text-yellow-500" />
              <h2 className="text-lg font-semibold">Potentially Oversized Clusters</h2>
            </div>
            <p className="text-sm text-muted-foreground mb-4">
              These clusters have 5+ workers and may have excess capacity based on estimated utilization.
            </p>

            {oversizedLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : sortedOversizedClusters.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <SortableHeader
                        label="Cluster"
                        field="cluster_name"
                        currentSort={oversizedSort}
                        onSort={handleOversizedSort}
                      />
                      <SortableHeader
                        label="Type"
                        field="cluster_type"
                        currentSort={oversizedSort}
                        onSort={handleOversizedSort}
                      />
                      <SortableHeader
                        label="Workers"
                        field="current_workers"
                        currentSort={oversizedSort}
                        onSort={handleOversizedSort}
                        align="center"
                      />
                      <SortableHeader
                        label="Efficiency"
                        field="avg_efficiency_score"
                        currentSort={oversizedSort}
                        onSort={handleOversizedSort}
                        align="center"
                      />
                      <SortableHeader
                        label="Recommended"
                        field="recommended_workers"
                        currentSort={oversizedSort}
                        onSort={handleOversizedSort}
                        align="center"
                      />
                      <SortableHeader
                        label="Monthly Savings"
                        field="potential_cost_savings"
                        currentSort={oversizedSort}
                        onSort={handleOversizedSort}
                        align="right"
                      />
                      <th className="text-right py-3 px-4 font-medium text-sm"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedOversizedClusters.map((cluster) => (
                      <tr key={cluster.cluster_id} className="border-b hover:bg-muted/50 transition-colors">
                        <td className="py-3 px-4">
                          <Link
                            to="/clusters/$clusterId"
                            params={{ clusterId: cluster.cluster_id }}
                            className="font-medium hover:text-primary"
                          >
                            {cluster.cluster_name}
                          </Link>
                        </td>
                        <td className="py-3 px-4">
                          <ClusterTypeBadge type={cluster.cluster_type} />
                        </td>
                        <td className="py-3 px-4 text-center">
                          <span className="font-medium">{cluster.current_workers}</span>
                        </td>
                        <td className="py-3 px-4 text-center">
                          <EfficiencyBadge score={cluster.avg_efficiency_score} />
                        </td>
                        <td className="py-3 px-4 text-center">
                          <span className="text-green-600 font-medium">{cluster.recommended_workers}</span>
                        </td>
                        <td className="py-3 px-4 text-right">
                          <span className="text-green-600 font-medium">
                            {formatCurrency(cluster.potential_cost_savings)}
                          </span>
                        </td>
                        <td className="py-3 px-4 text-right">
                          <ClusterActionsDropdown clusterId={cluster.cluster_id} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-center py-12 text-muted-foreground">
                <Lightbulb className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>No oversized clusters detected</p>
                <p className="text-sm mt-1">All clusters appear to be appropriately sized</p>
              </div>
            )}
          </div>
        )}

        {activeTab === "spark-config" && (
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Settings className="h-5 w-5 text-indigo-500" />
                <h2 className="text-lg font-semibold">Spark Configuration Analysis</h2>
              </div>
              {sparkConfigData && sparkConfigData.length > 0 && (
                <ViewToggle view={sparkConfigView} onViewChange={setSparkConfigView} />
              )}
            </div>
            <p className="text-sm text-muted-foreground mb-4">
              Analyze Spark configurations across all clusters and identify optimization opportunities for AQE, Photon, shuffle partitions, and more.
            </p>

            {sparkConfigLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : sparkConfigData && sparkConfigData.length > 0 ? (
              <div className="space-y-6">
                {/* Summary Stats */}
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <Cpu size={14} />
                      Clusters with Issues
                    </div>
                    <p className="text-2xl font-bold">{sparkConfigData.length}</p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <AlertTriangle size={14} />
                      Total Recommendations
                    </div>
                    <p className="text-2xl font-bold">
                      {sparkConfigData.reduce((sum, c) => sum + c.total_issues, 0)}
                    </p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <Zap size={14} />
                      Using Photon
                    </div>
                    <p className="text-2xl font-bold">
                      {sparkConfigData.filter((c) => c.is_photon_enabled).length} / {sparkConfigData.length}
                    </p>
                  </div>
                </div>

                {/* Impact Type Distribution - Clickable to filter */}
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  {[
                    { type: "performance", label: "Performance", icon: Zap },
                    { type: "cost", label: "Cost", icon: DollarSign },
                    { type: "reliability", label: "Reliability", icon: CheckCircle },
                    { type: "memory", label: "Memory", icon: Cpu },
                  ].map(({ type, label, icon: Icon }) => {
                    const count = sparkConfigData.reduce(
                      (sum, c) => sum + c.recommendations.filter((r) => r.impact === type).length,
                      0
                    );
                    const isActive = sparkConfigFilter?.type === "impact" && sparkConfigFilter?.value === type;
                    return (
                      <button
                        key={type}
                        onClick={() => {
                          if (isActive) {
                            setSparkConfigFilter(null);
                          } else if (count > 0) {
                            setSparkConfigFilter({ type: "impact", value: type });
                          }
                        }}
                        disabled={count === 0 && !isActive}
                        className={cn(
                          "bg-muted/30 rounded-lg p-3 text-center transition-all",
                          count > 0 && "hover:bg-muted/50 cursor-pointer",
                          count === 0 && "opacity-50 cursor-not-allowed",
                          isActive && "ring-2 ring-primary bg-primary/10"
                        )}
                      >
                        <Icon size={16} className={cn("mx-auto mb-1", isActive ? "text-primary" : "text-muted-foreground")} />
                        <p className="text-lg font-semibold">{count}</p>
                        <p className="text-xs text-muted-foreground">{label}</p>
                      </button>
                    );
                  })}
                </div>

                {/* Active Filter Indicator */}
                {sparkConfigFilter && (
                  <div className="flex items-center gap-2 p-3 bg-primary/10 rounded-lg border border-primary/20">
                    <span className="text-sm">
                      Filtering by:{" "}
                      <strong>
                        {{
                          performance: "Performance",
                          cost: "Cost",
                          reliability: "Reliability",
                          memory: "Memory",
                        }[sparkConfigFilter.value] || sparkConfigFilter.value}
                      </strong>
                    </span>
                    <span className="text-sm text-muted-foreground">
                      ({sortedSparkConfigData.length} cluster{sortedSparkConfigData.length !== 1 ? "s" : ""})
                    </span>
                    <button
                      onClick={() => setSparkConfigFilter(null)}
                      className="ml-auto p-1 rounded-md hover:bg-muted/50 text-muted-foreground hover:text-foreground"
                    >
                      <X size={16} />
                    </button>
                  </div>
                )}

                {/* Cluster Cards or List */}
                {sparkConfigView === "cards" ? (
                  sortedSparkConfigData.map((cluster) => (
                    <SparkConfigClusterCard key={cluster.cluster_id} cluster={cluster} />
                  ))
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="border-b bg-muted/50">
                          <GenericSortableHeader<SparkConfigSortField>
                            label="Cluster"
                            field="cluster_name"
                            currentSort={sparkConfigSort}
                            onSort={handleSparkConfigSort}
                          />
                          <GenericSortableHeader<SparkConfigSortField>
                            label="Photon"
                            field="is_photon_enabled"
                            currentSort={sparkConfigSort}
                            onSort={handleSparkConfigSort}
                          />
                          <GenericSortableHeader<SparkConfigSortField>
                            label="AQE"
                            field="aqe_enabled"
                            currentSort={sparkConfigSort}
                            onSort={handleSparkConfigSort}
                          />
                          <GenericSortableHeader<SparkConfigSortField>
                            label="Spark Version"
                            field="spark_version"
                            currentSort={sparkConfigSort}
                            onSort={handleSparkConfigSort}
                          />
                          <GenericSortableHeader<SparkConfigSortField>
                            label="Issues"
                            field="total_issues"
                            currentSort={sparkConfigSort}
                            onSort={handleSparkConfigSort}
                            align="center"
                          />
                          <th className="text-right py-3 px-4 font-medium text-sm"></th>
                        </tr>
                      </thead>
                      <tbody>
                        {sortedSparkConfigData.map((cluster) => (
                          <SparkConfigListRow key={cluster.cluster_id} cluster={cluster} />
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            ) : (
              <div className="text-center py-12 text-muted-foreground">
                <CheckCircle className="h-12 w-12 mx-auto mb-4 opacity-50 text-green-500" />
                <p>All clusters have optimal Spark configurations</p>
                <p className="text-sm mt-1">No configuration issues detected</p>
              </div>
            )}
          </div>
        )}

        {activeTab === "cost" && (
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <DollarSign className="h-5 w-5 text-green-500" />
                <h2 className="text-lg font-semibold">Cost Optimization</h2>
              </div>
              {costData && costData.length > 0 && (
                <ViewToggle view={costView} onViewChange={setCostView} />
              )}
            </div>
            <p className="text-sm text-muted-foreground mb-4">
              Identify cost-saving opportunities including Spot instances, node type optimization, storage, and autoscaling configurations.
            </p>

            {costLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : costData && costData.length > 0 ? (
              <div className="space-y-6">
                {/* Summary Stats */}
                <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <Server size={14} />
                      Clusters Analyzed
                    </div>
                    <p className="text-2xl font-bold">{costData.length}</p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <AlertTriangle size={14} />
                      Total Recommendations
                    </div>
                    <p className="text-2xl font-bold">
                      {costData.reduce((sum, c) => sum + c.total_recommendations, 0)}
                    </p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <Zap size={14} />
                      Using Spot/Preemptible
                    </div>
                    <p className="text-2xl font-bold">
                      {costData.filter((c) => c.uses_spot_instances).length} / {costData.length}
                    </p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <DollarSign size={14} />
                      Avg Potential Savings
                    </div>
                    <p className="text-2xl font-bold text-green-600">
                      {formatNumber(
                        costData.reduce((sum, c) => sum + c.total_potential_savings_percent, 0) / costData.length,
                        0
                      )}%
                    </p>
                  </div>
                </div>

                {/* Category Distribution - Clickable to filter */}
                <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
                  {[
                    { type: "spot_instances", label: "Spot Instances", icon: Zap },
                    { type: "node_type", label: "Node Type", icon: Server },
                    { type: "storage", label: "Storage", icon: HardDrive },
                    { type: "autoscaling", label: "Autoscaling", icon: TrendingUp },
                    { type: "serverless", label: "Serverless", icon: Cloud },
                  ].map(({ type, label, icon: Icon }) => {
                    const count = costData.reduce(
                      (sum, c) => sum + c.recommendations.filter((r) => r.category === type).length,
                      0
                    );
                    const isActive = costFilter?.type === "category" && costFilter?.value === type;
                    return (
                      <button
                        key={type}
                        onClick={() => {
                          if (isActive) {
                            setCostFilter(null);
                          } else if (count > 0) {
                            setCostFilter({ type: "category", value: type });
                          }
                        }}
                        disabled={count === 0 && !isActive}
                        className={cn(
                          "bg-muted/30 rounded-lg p-3 text-center transition-all",
                          count > 0 && "hover:bg-muted/50 cursor-pointer",
                          count === 0 && "opacity-50 cursor-not-allowed",
                          isActive && "ring-2 ring-primary bg-primary/10"
                        )}
                      >
                        <Icon size={16} className={cn("mx-auto mb-1", isActive ? "text-primary" : "text-muted-foreground")} />
                        <p className="text-lg font-semibold">{count}</p>
                        <p className="text-xs text-muted-foreground">{label}</p>
                      </button>
                    );
                  })}
                </div>

                {/* Active Filter Indicator */}
                {costFilter && (
                  <div className="flex items-center gap-2 p-3 bg-primary/10 rounded-lg border border-primary/20">
                    <span className="text-sm">
                      Filtering by:{" "}
                      <strong>
                        {{
                          spot_instances: "Spot Instances",
                          node_type: "Node Type",
                          storage: "Storage",
                          autoscaling: "Autoscaling",
                          serverless: "Serverless",
                        }[costFilter.value] || costFilter.value}
                      </strong>
                    </span>
                    <span className="text-sm text-muted-foreground">
                      ({sortedCostData.length} cluster{sortedCostData.length !== 1 ? "s" : ""})
                    </span>
                    <button
                      onClick={() => setCostFilter(null)}
                      className="ml-auto p-1 rounded-md hover:bg-muted/50 text-muted-foreground hover:text-foreground"
                    >
                      <X size={16} />
                    </button>
                  </div>
                )}

                {/* Cluster Cards or List */}
                {costView === "cards" ? (
                  sortedCostData.map((cluster) => (
                    <CostAnalysisClusterCard key={cluster.cluster_id} cluster={cluster} />
                  ))
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="border-b bg-muted/50">
                          <GenericSortableHeader<CostSortField>
                            label="Cluster"
                            field="cluster_name"
                            currentSort={costSort}
                            onSort={handleCostSort}
                          />
                          <GenericSortableHeader<CostSortField>
                            label="Cloud"
                            field="cloud_provider"
                            currentSort={costSort}
                            onSort={handleCostSort}
                          />
                          <GenericSortableHeader<CostSortField>
                            label="Spot"
                            field="uses_spot_instances"
                            currentSort={costSort}
                            onSort={handleCostSort}
                          />
                          <GenericSortableHeader<CostSortField>
                            label="Node Type"
                            field="node_type_id"
                            currentSort={costSort}
                            onSort={handleCostSort}
                          />
                          <GenericSortableHeader<CostSortField>
                            label="Workers"
                            field="num_workers"
                            currentSort={costSort}
                            onSort={handleCostSort}
                            align="center"
                          />
                          <GenericSortableHeader<CostSortField>
                            label="Issues"
                            field="total_recommendations"
                            currentSort={costSort}
                            onSort={handleCostSort}
                            align="center"
                          />
                          <GenericSortableHeader<CostSortField>
                            label="Savings"
                            field="total_potential_savings_percent"
                            currentSort={costSort}
                            onSort={handleCostSort}
                            align="right"
                          />
                          <th className="text-right py-3 px-4 font-medium text-sm"></th>
                        </tr>
                      </thead>
                      <tbody>
                        {sortedCostData.map((cluster) => (
                          <CostAnalysisListRow key={cluster.cluster_id} cluster={cluster} />
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            ) : (
              <div className="text-center py-12 text-muted-foreground">
                <CheckCircle className="h-12 w-12 mx-auto mb-4 opacity-50 text-green-500" />
                <p>All clusters are cost-optimized</p>
                <p className="text-sm mt-1">No cost optimization recommendations at this time</p>
              </div>
            )}
          </div>
        )}

        {activeTab === "autoscaling" && (
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <ArrowUpDown className="h-5 w-5 text-purple-500" />
                <h2 className="text-lg font-semibold">Autoscaling Analysis</h2>
              </div>
              {autoscalingData && autoscalingData.length > 0 && (
                <ViewToggle view={autoscalingView} onViewChange={setAutoscalingView} />
              )}
            </div>
            <p className="text-sm text-muted-foreground mb-4">
              Analyze autoscaling configurations for optimal cost efficiency. Detects wide/narrow ranges, high minimums, and missing auto-termination.
            </p>

            {autoscalingLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : autoscalingData && autoscalingData.length > 0 ? (
              <div className="space-y-6">
                {/* Summary Stats */}
                <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <Server size={14} />
                      Clusters Analyzed
                    </div>
                    <p className="text-2xl font-bold">{autoscalingData.length}</p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <AlertTriangle size={14} />
                      Total Issues
                    </div>
                    <p className="text-2xl font-bold">
                      {autoscalingData.reduce((sum, c) => sum + c.total_issues, 0)}
                    </p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <TrendingUp size={14} />
                      Using Autoscaling
                    </div>
                    <p className="text-2xl font-bold">
                      {autoscalingData.filter((c) => c.has_autoscaling).length} / {autoscalingData.length}
                    </p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <DollarSign size={14} />
                      Avg Potential Savings
                    </div>
                    <p className="text-2xl font-bold text-green-600">
                      {formatNumber(
                        autoscalingData.reduce((sum, c) => sum + c.total_potential_savings_percent, 0) / autoscalingData.length,
                        0
                      )}%
                    </p>
                  </div>
                </div>

                {/* Issue Type Distribution - Clickable to filter */}
                <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
                  {[
                    { type: "wide_range", label: "Wide Range", icon: TrendingUp },
                    { type: "narrow_range", label: "Narrow Range", icon: TrendingDown },
                    { type: "high_minimum", label: "High Minimum", icon: AlertTriangle },
                    { type: "no_autoscaling", label: "No Autoscaling", icon: Target },
                    { type: "inefficient_range", label: "Inefficient", icon: Settings },
                  ].map(({ type, label, icon: Icon }) => {
                    const count = autoscalingData.reduce(
                      (sum, c) => sum + c.recommendations.filter((r) => r.issue_type === type).length,
                      0
                    );
                    const isActive = autoscalingFilter?.type === "issue" && autoscalingFilter?.value === type;
                    return (
                      <button
                        key={type}
                        onClick={() => {
                          if (isActive) {
                            setAutoscalingFilter(null);
                          } else if (count > 0) {
                            setAutoscalingFilter({ type: "issue", value: type });
                          }
                        }}
                        disabled={count === 0 && !isActive}
                        className={cn(
                          "bg-muted/30 rounded-lg p-3 text-center transition-all",
                          count > 0 && "hover:bg-muted/50 cursor-pointer",
                          count === 0 && "opacity-50 cursor-not-allowed",
                          isActive && "ring-2 ring-primary bg-primary/10"
                        )}
                      >
                        <Icon size={16} className={cn("mx-auto mb-1", isActive ? "text-primary" : "text-muted-foreground")} />
                        <p className="text-lg font-semibold">{count}</p>
                        <p className="text-xs text-muted-foreground">{label}</p>
                      </button>
                    );
                  })}
                </div>

                {/* Active Filter Indicator */}
                {autoscalingFilter && (
                  <div className="flex items-center gap-2 p-3 bg-primary/10 rounded-lg border border-primary/20">
                    <span className="text-sm">
                      Filtering by:{" "}
                      <strong>
                        {{
                          wide_range: "Wide Range",
                          narrow_range: "Narrow Range",
                          high_minimum: "High Minimum",
                          no_autoscaling: "No Autoscaling",
                          inefficient_range: "Inefficient",
                        }[autoscalingFilter.value] || autoscalingFilter.value}
                      </strong>
                    </span>
                    <span className="text-sm text-muted-foreground">
                      ({sortedAutoscalingData.length} cluster{sortedAutoscalingData.length !== 1 ? "s" : ""})
                    </span>
                    <button
                      onClick={() => setAutoscalingFilter(null)}
                      className="ml-auto p-1 rounded-md hover:bg-muted/50 text-muted-foreground hover:text-foreground"
                    >
                      <X size={16} />
                    </button>
                  </div>
                )}

                {/* Cluster Cards or List */}
                {autoscalingView === "cards" ? (
                  sortedAutoscalingData.map((cluster) => (
                    <AutoscalingAnalysisClusterCard key={cluster.cluster_id} cluster={cluster} />
                  ))
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="border-b bg-muted/50">
                          <GenericSortableHeader<AutoscalingSortField>
                            label="Cluster"
                            field="cluster_name"
                            currentSort={autoscalingSort}
                            onSort={handleAutoscalingSort}
                          />
                          <GenericSortableHeader<AutoscalingSortField>
                            label="Type"
                            field="cluster_type"
                            currentSort={autoscalingSort}
                            onSort={handleAutoscalingSort}
                          />
                          <GenericSortableHeader<AutoscalingSortField>
                            label="Autoscale"
                            field="has_autoscaling"
                            currentSort={autoscalingSort}
                            onSort={handleAutoscalingSort}
                          />
                          <GenericSortableHeader<AutoscalingSortField>
                            label="Workers"
                            field="current_workers"
                            currentSort={autoscalingSort}
                            onSort={handleAutoscalingSort}
                            align="center"
                          />
                          <GenericSortableHeader<AutoscalingSortField>
                            label="Auto-Term"
                            field="auto_terminate_minutes"
                            currentSort={autoscalingSort}
                            onSort={handleAutoscalingSort}
                          />
                          <GenericSortableHeader<AutoscalingSortField>
                            label="Issues"
                            field="total_issues"
                            currentSort={autoscalingSort}
                            onSort={handleAutoscalingSort}
                            align="center"
                          />
                          <GenericSortableHeader<AutoscalingSortField>
                            label="Savings"
                            field="total_potential_savings_percent"
                            currentSort={autoscalingSort}
                            onSort={handleAutoscalingSort}
                            align="right"
                          />
                          <th className="text-right py-3 px-4 font-medium text-sm"></th>
                        </tr>
                      </thead>
                      <tbody>
                        {sortedAutoscalingData.map((cluster) => (
                          <AutoscalingAnalysisListRow key={cluster.cluster_id} cluster={cluster} />
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            ) : (
              <div className="text-center py-12 text-muted-foreground">
                <CheckCircle className="h-12 w-12 mx-auto mb-4 opacity-50 text-green-500" />
                <p>All clusters have optimal autoscaling configurations</p>
                <p className="text-sm mt-1">No autoscaling issues detected</p>
              </div>
            )}
          </div>
        )}

        {activeTab === "node-types" && (
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Server className="h-5 w-5 text-orange-500" />
                <h2 className="text-lg font-semibold">Node Type Analysis</h2>
              </div>
              {nodeTypeData && nodeTypeData.length > 0 && (
                <ViewToggle view={nodeTypeView} onViewChange={setNodeTypeView} />
              )}
            </div>
            <p className="text-sm text-muted-foreground mb-4">
              Analyze instance types for cost and performance optimization. Detects oversized drivers, GPU underutilization, legacy instances, and mismatched configurations.
            </p>

            {nodeTypeLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : nodeTypeData && nodeTypeData.length > 0 ? (
              <div className="space-y-6">
                {/* Summary Stats */}
                <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <Server size={14} />
                      Clusters Analyzed
                    </div>
                    <p className="text-2xl font-bold">{nodeTypeData.length}</p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <AlertTriangle size={14} />
                      Total Issues
                    </div>
                    <p className="text-2xl font-bold">
                      {nodeTypeData.reduce((sum, c) => sum + c.total_issues, 0)}
                    </p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <Cpu size={14} />
                      GPU Clusters
                    </div>
                    <p className="text-2xl font-bold">
                      {nodeTypeData.filter((c) => c.worker_node_category === "gpu").length}
                    </p>
                  </div>
                  <div className="bg-muted/50 rounded-lg p-4">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                      <DollarSign size={14} />
                      Avg Potential Savings
                    </div>
                    <p className="text-2xl font-bold text-green-600">
                      {formatNumber(
                        nodeTypeData.reduce((sum, c) => sum + c.total_potential_savings_percent, 0) / nodeTypeData.length,
                        0
                      )}%
                    </p>
                  </div>
                </div>

                {/* Issue Type Distribution - Clickable to filter */}
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  {[
                    { type: "oversized_driver", label: "Oversized Driver", icon: Server },
                    { type: "gpu_underutilized", label: "GPU Underutilized", icon: Cpu },
                    { type: "legacy_instance", label: "Legacy Instance", icon: Clock },
                    { type: "overprovisioned", label: "Overprovisioned", icon: TrendingUp },
                  ].map(({ type, label, icon: Icon }) => {
                    const count = nodeTypeData.reduce(
                      (sum, c) => sum + c.recommendations.filter((r) => r.issue_type === type).length,
                      0
                    );
                    const isActive = nodeTypeFilter?.type === "issue" && nodeTypeFilter?.value === type;
                    return (
                      <button
                        key={type}
                        onClick={() => {
                          if (isActive) {
                            setNodeTypeFilter(null);
                          } else if (count > 0) {
                            setNodeTypeFilter({ type: "issue", value: type });
                          }
                        }}
                        disabled={count === 0 && !isActive}
                        className={cn(
                          "bg-muted/30 rounded-lg p-3 text-center transition-all",
                          count > 0 && "hover:bg-muted/50 cursor-pointer",
                          count === 0 && "opacity-50 cursor-not-allowed",
                          isActive && "ring-2 ring-primary bg-primary/10"
                        )}
                      >
                        <Icon size={16} className={cn("mx-auto mb-1", isActive ? "text-primary" : "text-muted-foreground")} />
                        <p className="text-lg font-semibold">{count}</p>
                        <p className="text-xs text-muted-foreground">{label}</p>
                      </button>
                    );
                  })}
                </div>

                {/* Node Type Category Distribution - Clickable to filter */}
                <div className="p-4 bg-muted/30 rounded-lg">
                  <p className="text-sm font-medium mb-3">Instance Category Distribution</p>
                  <div className="flex flex-wrap gap-3">
                    {["memory_optimized", "compute_optimized", "general_purpose", "gpu", "storage_optimized", "unknown"].map((cat) => {
                      const count = nodeTypeData.filter((c) => c.worker_node_category === cat).length;
                      if (count === 0) return null;
                      const isActive = nodeTypeFilter?.type === "category" && nodeTypeFilter?.value === cat;
                      return (
                        <button
                          key={cat}
                          onClick={() => {
                            if (isActive) {
                              setNodeTypeFilter(null);
                            } else {
                              setNodeTypeFilter({ type: "category", value: cat });
                            }
                          }}
                          className={cn(
                            "flex items-center gap-2 px-2 py-1 rounded-md transition-all hover:bg-muted/50 cursor-pointer",
                            isActive && "ring-2 ring-primary bg-primary/10"
                          )}
                        >
                          <NodeTypeCategoryBadge category={cat as NodeTypeCategory} />
                          <span className="text-sm text-muted-foreground">{count}</span>
                        </button>
                      );
                    })}
                  </div>
                </div>

                {/* Active Filter Indicator */}
                {nodeTypeFilter && (
                  <div className="flex items-center gap-2 p-3 bg-primary/10 rounded-lg border border-primary/20">
                    <span className="text-sm">
                      Filtering by:{" "}
                      <strong>
                        {nodeTypeFilter.type === "issue"
                          ? {
                              oversized_driver: "Oversized Driver",
                              gpu_underutilized: "GPU Underutilized",
                              legacy_instance: "Legacy Instance",
                              overprovisioned: "Overprovisioned",
                            }[nodeTypeFilter.value] || nodeTypeFilter.value
                          : {
                              memory_optimized: "Memory Optimized",
                              compute_optimized: "Compute Optimized",
                              general_purpose: "General Purpose",
                              gpu: "GPU",
                              storage_optimized: "Storage Optimized",
                              unknown: "Unknown",
                            }[nodeTypeFilter.value] || nodeTypeFilter.value}
                      </strong>
                    </span>
                    <span className="text-sm text-muted-foreground">
                      ({sortedNodeTypeData.length} cluster{sortedNodeTypeData.length !== 1 ? "s" : ""})
                    </span>
                    <button
                      onClick={() => setNodeTypeFilter(null)}
                      className="ml-auto p-1 rounded-md hover:bg-muted/50 text-muted-foreground hover:text-foreground"
                    >
                      <X size={16} />
                    </button>
                  </div>
                )}

                {/* Cluster Cards or List */}
                {nodeTypeView === "cards" ? (
                  sortedNodeTypeData.map((cluster) => (
                    <NodeTypeAnalysisClusterCard key={cluster.cluster_id} cluster={cluster} />
                  ))
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="border-b bg-muted/50">
                          <GenericSortableHeader<NodeTypeSortField>
                            label="Cluster"
                            field="cluster_name"
                            currentSort={nodeTypeSort}
                            onSort={handleNodeTypeSort}
                          />
                          <GenericSortableHeader<NodeTypeSortField>
                            label="Type"
                            field="cluster_type"
                            currentSort={nodeTypeSort}
                            onSort={handleNodeTypeSort}
                          />
                          <GenericSortableHeader<NodeTypeSortField>
                            label="Cloud"
                            field="cloud_provider"
                            currentSort={nodeTypeSort}
                            onSort={handleNodeTypeSort}
                          />
                          <GenericSortableHeader<NodeTypeSortField>
                            label="Category"
                            field="worker_node_category"
                            currentSort={nodeTypeSort}
                            onSort={handleNodeTypeSort}
                          />
                          <GenericSortableHeader<NodeTypeSortField>
                            label="Node Type"
                            field="worker_node_type"
                            currentSort={nodeTypeSort}
                            onSort={handleNodeTypeSort}
                          />
                          <GenericSortableHeader<NodeTypeSortField>
                            label="Issues"
                            field="total_issues"
                            currentSort={nodeTypeSort}
                            onSort={handleNodeTypeSort}
                            align="center"
                          />
                          <GenericSortableHeader<NodeTypeSortField>
                            label="Savings"
                            field="total_potential_savings_percent"
                            currentSort={nodeTypeSort}
                            onSort={handleNodeTypeSort}
                            align="right"
                          />
                          <th className="text-right py-3 px-4 font-medium text-sm"></th>
                        </tr>
                      </thead>
                      <tbody>
                        {sortedNodeTypeData.map((cluster) => (
                          <NodeTypeAnalysisListRow key={cluster.cluster_id} cluster={cluster} />
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            ) : (
              <div className="text-center py-12 text-muted-foreground">
                <CheckCircle className="h-12 w-12 mx-auto mb-4 opacity-50 text-green-500" />
                <p>All clusters have optimal node type configurations</p>
                <p className="text-sm mt-1">No node type issues detected</p>
              </div>
            )}
          </div>
        )}

        {activeTab === "jobs" && (
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Play className="h-5 w-5 text-blue-500" />
                <h2 className="text-lg font-semibold">Job Cluster Recommendations</h2>
              </div>
              {jobRecommendations && jobRecommendations.length > 0 && (
                <ViewToggle view={jobsView} onViewChange={setJobsView} />
              )}
            </div>
            <p className="text-sm text-muted-foreground mb-4">
              Suggestions to consolidate job workloads onto underutilized clusters.
            </p>

            {jobsLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : jobRecommendations && jobRecommendations.length > 0 ? (
              jobsView === "cards" ? (
                <div className="space-y-4">
                  {jobRecommendations.map((rec, idx) => (
                    <div
                      key={idx}
                      className="p-4 bg-muted/50 rounded-lg border border-transparent hover:border-primary/20 transition-colors"
                    >
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-4">
                          <div className="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
                            <Zap className="h-5 w-5 text-blue-600" />
                          </div>
                          <div>
                            <p className="font-medium">
                              Move jobs from{" "}
                              <Link
                                to="/clusters/$clusterId"
                                params={{ clusterId: rec.source_cluster_id }}
                                className="text-primary hover:underline"
                              >
                                {rec.source_cluster_name}
                              </Link>
                            </p>
                            <p className="text-sm text-muted-foreground mt-0.5">
                              Target:{" "}
                              <Link
                                to="/clusters/$clusterId"
                                params={{ clusterId: rec.target_cluster_id }}
                                className="text-primary hover:underline"
                              >
                                {rec.target_cluster_name}
                              </Link>
                            </p>
                          </div>
                        </div>
                        <span className="text-sm text-green-600 font-medium">{rec.estimated_savings}</span>
                      </div>
                      <p className="text-sm text-muted-foreground mt-3">{rec.reason}</p>
                      <div className="flex items-center gap-2 mt-2">
                        <Users size={14} className="text-muted-foreground" />
                        <span className="text-sm text-muted-foreground">{rec.job_count} jobs could be moved</span>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b bg-muted/50">
                        <GenericSortableHeader<JobsSortField>
                          label="Source Cluster"
                          field="source_cluster_name"
                          currentSort={jobsSort}
                          onSort={handleJobsSort}
                        />
                        <GenericSortableHeader<JobsSortField>
                          label="Target Cluster"
                          field="target_cluster_name"
                          currentSort={jobsSort}
                          onSort={handleJobsSort}
                        />
                        <GenericSortableHeader<JobsSortField>
                          label="Jobs"
                          field="job_count"
                          currentSort={jobsSort}
                          onSort={handleJobsSort}
                          align="center"
                        />
                        <th className="text-left py-3 px-4 font-medium text-sm">Reason</th>
                        <GenericSortableHeader<JobsSortField>
                          label="Savings"
                          field="estimated_savings"
                          currentSort={jobsSort}
                          onSort={handleJobsSort}
                          align="right"
                        />
                      </tr>
                    </thead>
                    <tbody>
                      {sortedJobRecommendations.map((rec, idx) => (
                        <tr key={idx} className="border-b hover:bg-muted/50 transition-colors">
                          <td className="py-3 px-4">
                            <Link
                              to="/clusters/$clusterId"
                              params={{ clusterId: rec.source_cluster_id }}
                              className="font-medium hover:text-primary"
                            >
                              {rec.source_cluster_name}
                            </Link>
                          </td>
                          <td className="py-3 px-4">
                            <Link
                              to="/clusters/$clusterId"
                              params={{ clusterId: rec.target_cluster_id }}
                              className="hover:text-primary"
                            >
                              {rec.target_cluster_name}
                            </Link>
                          </td>
                          <td className="py-3 px-4 text-center">{rec.job_count}</td>
                          <td className="py-3 px-4 text-sm text-muted-foreground max-w-xs truncate">
                            {rec.reason}
                          </td>
                          <td className="py-3 px-4 text-right">
                            <span className="text-green-600 font-medium">{rec.estimated_savings}</span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )
            ) : (
              <div className="text-center py-12 text-muted-foreground">
                <Play className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>No job recommendations at this time</p>
                <p className="text-sm mt-1">Job workloads appear to be well distributed</p>
              </div>
            )}
          </div>
        )}

        {activeTab === "schedule" && (
          <div className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Calendar className="h-5 w-5 text-purple-500" />
                <h2 className="text-lg font-semibold">Schedule Optimization</h2>
              </div>
              {scheduleRecommendations && scheduleRecommendations.length > 0 && (
                <ViewToggle view={scheduleView} onViewChange={setScheduleView} />
              )}
            </div>
            <p className="text-sm text-muted-foreground mb-4">
              Recommendations to optimize auto-termination and idle time settings.
            </p>

            {scheduleLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : scheduleRecommendations && scheduleRecommendations.length > 0 ? (
              scheduleView === "cards" ? (
                <div className="space-y-4">
                  {scheduleRecommendations.map((rec, idx) => (
                    <div
                      key={idx}
                      className="p-4 bg-muted/50 rounded-lg border border-transparent hover:border-primary/20 transition-colors"
                    >
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-4">
                          <div className="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
                            <Clock className="h-5 w-5 text-purple-600" />
                          </div>
                          <div>
                            <Link
                              to="/clusters/$clusterId"
                              params={{ clusterId: rec.cluster_id }}
                              className="font-medium hover:text-primary"
                            >
                              {rec.cluster_name}
                            </Link>
                            <p className="text-sm text-muted-foreground mt-0.5">
                              Current auto-terminate:{" "}
                              {rec.current_auto_terminate_minutes
                                ? `${rec.current_auto_terminate_minutes} min`
                                : "Not configured"}
                            </p>
                          </div>
                        </div>
                        <div className="text-right">
                          <span className="text-sm text-green-600 font-medium">
                            Recommended: {rec.recommended_auto_terminate_minutes} min
                          </span>
                          <p className="text-xs text-muted-foreground mt-0.5">
                            ~{formatNumber(rec.avg_idle_time_per_day_minutes, 0)} min idle/day
                          </p>
                        </div>
                      </div>
                      <p className="text-sm text-muted-foreground mt-3">{rec.reason}</p>
                      {rec.peak_usage_hours && rec.peak_usage_hours.length > 0 && (
                        <div className="flex items-center gap-2 mt-2">
                          <Clock size={14} className="text-muted-foreground" />
                          <span className="text-sm text-muted-foreground">
                            Peak hours: {rec.peak_usage_hours.map((h) => `${h}:00`).join(", ")}
                          </span>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b bg-muted/50">
                        <GenericSortableHeader<ScheduleSortField>
                          label="Cluster"
                          field="cluster_name"
                          currentSort={scheduleSort}
                          onSort={handleScheduleSort}
                        />
                        <GenericSortableHeader<ScheduleSortField>
                          label="Current"
                          field="current_auto_terminate_minutes"
                          currentSort={scheduleSort}
                          onSort={handleScheduleSort}
                          align="center"
                        />
                        <GenericSortableHeader<ScheduleSortField>
                          label="Recommended"
                          field="recommended_auto_terminate_minutes"
                          currentSort={scheduleSort}
                          onSort={handleScheduleSort}
                          align="center"
                        />
                        <GenericSortableHeader<ScheduleSortField>
                          label="Avg Idle/Day"
                          field="avg_idle_time_per_day_minutes"
                          currentSort={scheduleSort}
                          onSort={handleScheduleSort}
                          align="center"
                        />
                        <th className="text-left py-3 px-4 font-medium text-sm">Peak Hours</th>
                        <th className="text-right py-3 px-4 font-medium text-sm"></th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedScheduleRecommendations.map((rec, idx) => (
                        <tr key={idx} className="border-b hover:bg-muted/50 transition-colors">
                          <td className="py-3 px-4">
                            <Link
                              to="/clusters/$clusterId"
                              params={{ clusterId: rec.cluster_id }}
                              className="font-medium hover:text-primary"
                            >
                              {rec.cluster_name}
                            </Link>
                          </td>
                          <td className="py-3 px-4 text-center">
                            {rec.current_auto_terminate_minutes
                              ? `${rec.current_auto_terminate_minutes} min`
                              : <span className="text-muted-foreground">None</span>}
                          </td>
                          <td className="py-3 px-4 text-center">
                            <span className="text-green-600 font-medium">
                              {rec.recommended_auto_terminate_minutes} min
                            </span>
                          </td>
                          <td className="py-3 px-4 text-center text-muted-foreground">
                            {formatNumber(rec.avg_idle_time_per_day_minutes, 0)} min
                          </td>
                          <td className="py-3 px-4 text-sm text-muted-foreground">
                            {rec.peak_usage_hours && rec.peak_usage_hours.length > 0
                              ? rec.peak_usage_hours.map((h) => `${h}:00`).join(", ")
                              : "-"}
                          </td>
                          <td className="py-3 px-4 text-right">
                            <ClusterActionsDropdown clusterId={rec.cluster_id} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )
            ) : (
              <div className="text-center py-12 text-muted-foreground">
                <Calendar className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>No schedule optimizations needed</p>
                <p className="text-sm mt-1">Auto-termination settings look good</p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Info Note */}
      <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
        <div className="flex items-start gap-3">
          <Lightbulb className="h-5 w-5 text-blue-500 flex-shrink-0 mt-0.5" />
          <div>
            <p className="text-sm font-medium text-blue-800 dark:text-blue-200">
              About Efficiency Scores
            </p>
            <p className="text-sm text-blue-700 dark:text-blue-300 mt-1">
              Efficiency is calculated as actual DBU consumption vs. theoretical maximum (cluster capacity × uptime).
              Scores below 30% indicate potentially oversized clusters. Click "Collect Metrics" to gather
              and persist daily utilization data for trend analysis.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export const Route = createFileRoute("/_sidebar/optimization")({
  component: OptimizationPage,
});
