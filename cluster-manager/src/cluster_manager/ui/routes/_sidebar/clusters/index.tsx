import { createFileRoute, Link } from "@tanstack/react-router";
import {
  Activity,
  AlertCircle,
  ChevronRight,
  Clock,
  Loader2,
  Play,
  RefreshCw,
  Square,
  Users,
  Zap,
} from "lucide-react";
import { toast } from "sonner";

import {
  ClusterSummary,
  useClusters,
  useMetricsSummary,
  useStartCluster,
  useStopCluster,
} from "@/lib/api";
import { cn, formatDuration, formatNumber } from "@/lib/utils";

const stateColors: Record<string, { bg: string; text: string; dot: string }> = {
  RUNNING: { bg: "bg-green-100 dark:bg-green-900/30", text: "text-green-700 dark:text-green-400", dot: "bg-green-500" },
  PENDING: { bg: "bg-yellow-100 dark:bg-yellow-900/30", text: "text-yellow-700 dark:text-yellow-400", dot: "bg-yellow-500" },
  RESTARTING: { bg: "bg-blue-100 dark:bg-blue-900/30", text: "text-blue-700 dark:text-blue-400", dot: "bg-blue-500" },
  RESIZING: { bg: "bg-blue-100 dark:bg-blue-900/30", text: "text-blue-700 dark:text-blue-400", dot: "bg-blue-500" },
  TERMINATING: { bg: "bg-orange-100 dark:bg-orange-900/30", text: "text-orange-700 dark:text-orange-400", dot: "bg-orange-500" },
  TERMINATED: { bg: "bg-gray-100 dark:bg-gray-800", text: "text-gray-600 dark:text-gray-400", dot: "bg-gray-400" },
  ERROR: { bg: "bg-red-100 dark:bg-red-900/30", text: "text-red-700 dark:text-red-400", dot: "bg-red-500" },
  UNKNOWN: { bg: "bg-gray-100 dark:bg-gray-800", text: "text-gray-600 dark:text-gray-400", dot: "bg-gray-400" },
};

function StatusBadge({ state }: { state: string }) {
  const colors = stateColors[state] || stateColors.UNKNOWN;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium",
        colors.bg,
        colors.text
      )}
    >
      <span className={cn("w-1.5 h-1.5 rounded-full", colors.dot)} />
      {state}
    </span>
  );
}

function MetricsCard({
  title,
  value,
  icon: Icon,
  subtitle,
}: {
  title: string;
  value: string | number;
  icon: React.ElementType;
  subtitle?: string;
}) {
  return (
    <div className="bg-card rounded-lg border p-4">
      <div className="flex items-center gap-3">
        <div className="p-2 bg-primary/10 rounded-lg">
          <Icon className="h-5 w-5 text-primary" />
        </div>
        <div>
          <p className="text-sm text-muted-foreground">{title}</p>
          <p className="text-2xl font-semibold">{value}</p>
          {subtitle && <p className="text-xs text-muted-foreground">{subtitle}</p>}
        </div>
      </div>
    </div>
  );
}

function ClusterCard({ cluster }: { cluster: ClusterSummary }) {
  const startCluster = useStartCluster();
  const stopCluster = useStopCluster();

  const isRunning = cluster.state === "RUNNING";
  const isTerminated = cluster.state === "TERMINATED";
  const isTransitioning = ["PENDING", "RESTARTING", "RESIZING", "TERMINATING"].includes(
    cluster.state
  );

  const handleStart = () => {
    startCluster.mutate(cluster.cluster_id, {
      onSuccess: (data) => {
        toast.success(data.message);
      },
      onError: (error) => {
        toast.error(`Failed to start cluster: ${error.message}`);
      },
    });
  };

  const handleStop = () => {
    stopCluster.mutate(cluster.cluster_id, {
      onSuccess: (data) => {
        toast.success(data.message);
      },
      onError: (error) => {
        toast.error(`Failed to stop cluster: ${error.message}`);
      },
    });
  };

  const workersDisplay = cluster.autoscale
    ? `${cluster.autoscale.min_workers}-${cluster.autoscale.max_workers}`
    : cluster.num_workers ?? 0;

  return (
    <div className="bg-card rounded-lg border hover:border-primary/50 transition-colors">
      <div className="p-4">
        <div className="flex items-start justify-between mb-3">
          <div className="flex-1 min-w-0">
            <Link
              to="/clusters/$clusterId"
              params={{ clusterId: cluster.cluster_id }}
              className="font-medium hover:text-primary truncate block"
            >
              {cluster.cluster_name}
            </Link>
            <p className="text-xs text-muted-foreground truncate mt-0.5">
              {cluster.creator_user_name || "Unknown creator"}
            </p>
          </div>
          <StatusBadge state={cluster.state} />
        </div>

        <div className="grid grid-cols-2 gap-3 text-sm mb-4">
          <div className="flex items-center gap-2 text-muted-foreground">
            <Users size={14} />
            <span>{workersDisplay} workers</span>
          </div>
          <div className="flex items-center gap-2 text-muted-foreground">
            <Clock size={14} />
            <span>
              {cluster.uptime_minutes > 0 ? formatDuration(cluster.uptime_minutes) : "-"}
            </span>
          </div>
          <div className="flex items-center gap-2 text-muted-foreground">
            <Zap size={14} />
            <span>
              {cluster.estimated_dbu_per_hour > 0
                ? `${formatNumber(cluster.estimated_dbu_per_hour)} DBU/h`
                : "-"}
            </span>
          </div>
          <div className="flex items-center gap-2 text-muted-foreground">
            <Activity size={14} />
            <span className="truncate">{cluster.spark_version?.split("-")[0] || "-"}</span>
          </div>
        </div>

        <div className="flex gap-2">
          <button
            onClick={handleStart}
            disabled={!isTerminated || startCluster.isPending}
            className={cn(
              "flex-1 flex items-center justify-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-colors",
              isTerminated
                ? "bg-green-600 hover:bg-green-700 text-white"
                : "bg-muted text-muted-foreground cursor-not-allowed"
            )}
          >
            {startCluster.isPending ? (
              <Loader2 size={14} className="animate-spin" />
            ) : (
              <Play size={14} />
            )}
            Start
          </button>
          <button
            onClick={handleStop}
            disabled={!isRunning || isTransitioning || stopCluster.isPending}
            className={cn(
              "flex-1 flex items-center justify-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-colors",
              isRunning && !isTransitioning
                ? "bg-secondary hover:bg-secondary/80 text-secondary-foreground"
                : "bg-muted text-muted-foreground cursor-not-allowed"
            )}
          >
            {stopCluster.isPending ? (
              <Loader2 size={14} className="animate-spin" />
            ) : (
              <Square size={14} />
            )}
            Stop
          </button>
          <Link
            to="/clusters/$clusterId"
            params={{ clusterId: cluster.cluster_id }}
            className="p-1.5 rounded-md hover:bg-muted transition-colors"
          >
            <ChevronRight size={18} />
          </Link>
        </div>
      </div>
    </div>
  );
}

function ClustersPage() {
  const { data: clusters, isLoading, error, refetch } = useClusters();
  const { data: metrics } = useMetricsSummary();

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center py-12">
        <AlertCircle className="h-12 w-12 text-destructive mb-4" />
        <h2 className="text-lg font-semibold mb-2">Failed to load clusters</h2>
        <p className="text-muted-foreground mb-4">{error.message}</p>
        <button
          onClick={() => refetch()}
          className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg"
        >
          <RefreshCw size={16} />
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Clusters</h1>
          <p className="text-muted-foreground">Manage and monitor your Databricks clusters</p>
        </div>
        <button
          onClick={() => refetch()}
          className="flex items-center gap-2 px-3 py-2 text-sm bg-secondary hover:bg-secondary/80 rounded-lg transition-colors"
        >
          <RefreshCw size={16} />
          Refresh
        </button>
      </div>

      {/* Metrics Summary */}
      {metrics && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <MetricsCard
            title="Total Clusters"
            value={metrics.total_clusters}
            icon={Activity}
          />
          <MetricsCard
            title="Running"
            value={metrics.running_clusters}
            icon={Play}
            subtitle={`${metrics.pending_clusters} pending`}
          />
          <MetricsCard
            title="Active Workers"
            value={metrics.total_running_workers}
            icon={Users}
          />
          <MetricsCard
            title="Est. Hourly DBU"
            value={formatNumber(metrics.estimated_hourly_dbu)}
            icon={Zap}
            subtitle="across all running clusters"
          />
        </div>
      )}

      {/* Cluster Grid */}
      {isLoading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </div>
      ) : clusters && clusters.length > 0 ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {clusters.map((cluster) => (
            <ClusterCard key={cluster.cluster_id} cluster={cluster} />
          ))}
        </div>
      ) : (
        <div className="text-center py-12">
          <Activity className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
          <h2 className="text-lg font-semibold mb-2">No clusters found</h2>
          <p className="text-muted-foreground">
            Create a cluster in your Databricks workspace to see it here.
          </p>
        </div>
      )}
    </div>
  );
}

export const Route = createFileRoute("/_sidebar/clusters/")({
  component: ClustersPage,
});
