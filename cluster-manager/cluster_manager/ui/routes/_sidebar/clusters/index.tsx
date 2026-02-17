import { useMemo, useState } from "react";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import {
  Activity,
  AlertCircle,
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  Clock,
  Grid3X3,
  List,
  Loader2,
  Play,
  RefreshCw,
  Shield,
  Square,
  Users,
  X,
  Zap,
} from "lucide-react";
import { toast } from "sonner";

import {
  ClusterSummary,
  useClusters,
  useMetricsSummary,
  usePolicies,
  useStartCluster,
  useStopCluster,
} from "@/lib/api";
import { cn, formatDuration, formatNumber } from "@/lib/utils";
import { ClusterActionsDropdown } from "@/components/clusters/cluster-actions-dropdown";

// Search params validation
interface ClusterSearchParams {
  policy?: string;
}

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
          <ClusterActionsDropdown
            clusterId={cluster.cluster_id}
            clusterType={cluster.cluster_source || undefined}
          />
        </div>
      </div>
    </div>
  );
}

function ClusterTableRow({ cluster, policyMap }: { cluster: ClusterSummary; policyMap: Map<string, string> }) {
  const startCluster = useStartCluster();
  const stopCluster = useStopCluster();

  const isRunning = cluster.state === "RUNNING";
  const isTerminated = cluster.state === "TERMINATED";
  const isTransitioning = ["PENDING", "RESTARTING", "RESIZING", "TERMINATING"].includes(
    cluster.state
  );

  const handleStart = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    startCluster.mutate(cluster.cluster_id, {
      onSuccess: (data) => {
        toast.success(data.message);
      },
      onError: (error) => {
        toast.error(`Failed to start cluster: ${error.message}`);
      },
    });
  };

  const handleStop = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
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
        <StatusBadge state={cluster.state} />
      </td>
      <td className="py-3 px-4 text-sm text-muted-foreground truncate max-w-[200px]">
        {cluster.creator_user_name || "-"}
      </td>
      <td className="py-3 px-4 text-sm truncate max-w-[150px]">
        {cluster.policy_id ? (
          <Link
            to="/policies"
            className="text-muted-foreground hover:text-primary hover:underline"
          >
            {policyMap.get(cluster.policy_id) || "-"}
          </Link>
        ) : (
          <span className="text-muted-foreground">-</span>
        )}
      </td>
      <td className="py-3 px-4 text-sm text-center">
        {workersDisplay}
      </td>
      <td className="py-3 px-4 text-sm text-center">
        {cluster.uptime_minutes > 0 ? formatDuration(cluster.uptime_minutes) : "-"}
      </td>
      <td className="py-3 px-4 text-sm text-center">
        {cluster.estimated_dbu_per_hour > 0 ? formatNumber(cluster.estimated_dbu_per_hour) : "-"}
      </td>
      <td className="py-3 px-4 text-sm text-muted-foreground">
        {cluster.spark_version?.split("-")[0] || "-"}
      </td>
      <td className="py-3 px-4">
        <div className="flex items-center gap-1">
          <button
            onClick={handleStart}
            disabled={!isTerminated || startCluster.isPending}
            title="Start cluster"
            className={cn(
              "p-1.5 rounded-md transition-colors",
              isTerminated
                ? "text-green-600 hover:bg-green-100 dark:hover:bg-green-900/30"
                : "text-muted-foreground/50 cursor-not-allowed"
            )}
          >
            {startCluster.isPending ? (
              <Loader2 size={16} className="animate-spin" />
            ) : (
              <Play size={16} />
            )}
          </button>
          <button
            onClick={handleStop}
            disabled={!isRunning || isTransitioning || stopCluster.isPending}
            title="Stop cluster"
            className={cn(
              "p-1.5 rounded-md transition-colors",
              isRunning && !isTransitioning
                ? "text-orange-600 hover:bg-orange-100 dark:hover:bg-orange-900/30"
                : "text-muted-foreground/50 cursor-not-allowed"
            )}
          >
            {stopCluster.isPending ? (
              <Loader2 size={16} className="animate-spin" />
            ) : (
              <Square size={16} />
            )}
          </button>
          <ClusterActionsDropdown
            clusterId={cluster.cluster_id}
            clusterType={cluster.cluster_source || undefined}
          />
        </div>
      </td>
    </tr>
  );
}

type SortColumn = "name" | "state" | "creator" | "policy" | "workers" | "uptime" | "dbu" | "runtime";
type SortDirection = "asc" | "desc";

interface SortState {
  column: SortColumn;
  direction: SortDirection;
}

function SortableHeader({
  column,
  label,
  currentSort,
  onSort,
  align = "left",
}: {
  column: SortColumn;
  label: string;
  currentSort: SortState;
  onSort: (column: SortColumn) => void;
  align?: "left" | "center";
}) {
  const isActive = currentSort.column === column;

  return (
    <th
      className={cn(
        "py-3 px-4 font-medium text-sm cursor-pointer hover:bg-muted/80 transition-colors select-none",
        align === "center" ? "text-center" : "text-left"
      )}
      onClick={() => onSort(column)}
    >
      <div className={cn("flex items-center gap-1", align === "center" && "justify-center")}>
        <span>{label}</span>
        {isActive ? (
          currentSort.direction === "asc" ? (
            <ArrowUp size={14} className="text-primary" />
          ) : (
            <ArrowDown size={14} className="text-primary" />
          )
        ) : (
          <ArrowUpDown size={14} className="text-muted-foreground/50" />
        )}
      </div>
    </th>
  );
}

function getWorkerCount(cluster: ClusterSummary): number {
  if (cluster.autoscale) {
    return (cluster.autoscale.min_workers + cluster.autoscale.max_workers) / 2;
  }
  return cluster.num_workers ?? 0;
}

const stateOrder: Record<string, number> = {
  RUNNING: 0,
  PENDING: 1,
  RESTARTING: 2,
  RESIZING: 3,
  TERMINATING: 4,
  TERMINATED: 5,
  ERROR: 6,
  UNKNOWN: 7,
};

function ClusterTable({ clusters, policyMap }: { clusters: ClusterSummary[]; policyMap: Map<string, string> }) {
  const [sort, setSort] = useState<SortState>({ column: "state", direction: "asc" });

  const handleSort = (column: SortColumn) => {
    setSort((prev) => ({
      column,
      direction: prev.column === column && prev.direction === "asc" ? "desc" : "asc",
    }));
  };

  const sortedClusters = useMemo(() => {
    const sorted = [...clusters].sort((a, b) => {
      let comparison = 0;

      switch (sort.column) {
        case "name":
          comparison = (a.cluster_name || "").localeCompare(b.cluster_name || "");
          break;
        case "state":
          comparison = (stateOrder[a.state] ?? 99) - (stateOrder[b.state] ?? 99);
          break;
        case "creator":
          comparison = (a.creator_user_name || "").localeCompare(b.creator_user_name || "");
          break;
        case "policy":
          const policyA = a.policy_id ? policyMap.get(a.policy_id) || "" : "";
          const policyB = b.policy_id ? policyMap.get(b.policy_id) || "" : "";
          comparison = policyA.localeCompare(policyB);
          break;
        case "workers":
          comparison = getWorkerCount(a) - getWorkerCount(b);
          break;
        case "uptime":
          comparison = (a.uptime_minutes || 0) - (b.uptime_minutes || 0);
          break;
        case "dbu":
          comparison = (a.estimated_dbu_per_hour || 0) - (b.estimated_dbu_per_hour || 0);
          break;
        case "runtime":
          comparison = (a.spark_version || "").localeCompare(b.spark_version || "");
          break;
      }

      return sort.direction === "asc" ? comparison : -comparison;
    });

    return sorted;
  }, [clusters, sort]);

  return (
    <div className="bg-card rounded-lg border overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="border-b bg-muted/50">
              <SortableHeader column="name" label="Name" currentSort={sort} onSort={handleSort} />
              <SortableHeader column="state" label="Status" currentSort={sort} onSort={handleSort} />
              <SortableHeader column="creator" label="Creator" currentSort={sort} onSort={handleSort} />
              <SortableHeader column="policy" label="Policy" currentSort={sort} onSort={handleSort} />
              <SortableHeader column="workers" label="Workers" currentSort={sort} onSort={handleSort} align="center" />
              <SortableHeader column="uptime" label="Uptime" currentSort={sort} onSort={handleSort} align="center" />
              <SortableHeader column="dbu" label="DBU/h" currentSort={sort} onSort={handleSort} align="center" />
              <SortableHeader column="runtime" label="Runtime" currentSort={sort} onSort={handleSort} />
              <th className="text-left py-3 px-4 font-medium text-sm w-[100px]">Actions</th>
            </tr>
          </thead>
          <tbody>
            {sortedClusters.map((cluster) => (
              <ClusterTableRow key={cluster.cluster_id} cluster={cluster} policyMap={policyMap} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

type ViewMode = "grid" | "list";

function ClustersPage() {
  const [viewMode, setViewMode] = useState<ViewMode>("list");
  const { policy: policyFilter } = Route.useSearch();
  const navigate = useNavigate();
  const { data: clusters, isLoading, error, refetch } = useClusters();
  const { data: metrics } = useMetricsSummary();
  const { data: policies } = usePolicies();

  // Create policy map for displaying policy names
  const policyMap = useMemo(() => {
    const map = new Map<string, string>();
    if (policies) {
      policies.forEach((p) => map.set(p.policy_id, p.name));
    }
    return map;
  }, [policies]);

  // Get the selected policy name
  const selectedPolicy = useMemo(() => {
    if (!policyFilter || !policies) return null;
    return policies.find((p) => p.policy_id === policyFilter);
  }, [policyFilter, policies]);

  // Filter clusters by policy if a policy filter is active
  const filteredClusters = useMemo(() => {
    if (!clusters) return [];
    if (!policyFilter) return clusters;
    return clusters.filter((cluster) => cluster.policy_id === policyFilter);
  }, [clusters, policyFilter]);

  const clearPolicyFilter = () => {
    navigate({ to: "/clusters", search: {} });
  };

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
        <div className="flex items-center gap-2">
          {/* View Toggle */}
          <div className="flex items-center bg-muted rounded-lg p-1">
            <button
              onClick={() => setViewMode("list")}
              title="List view"
              className={cn(
                "p-1.5 rounded-md transition-colors",
                viewMode === "list"
                  ? "bg-background shadow-sm text-foreground"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              <List size={18} />
            </button>
            <button
              onClick={() => setViewMode("grid")}
              title="Grid view"
              className={cn(
                "p-1.5 rounded-md transition-colors",
                viewMode === "grid"
                  ? "bg-background shadow-sm text-foreground"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              <Grid3X3 size={18} />
            </button>
          </div>
          <button
            onClick={() => refetch()}
            className="flex items-center gap-2 px-3 py-2 text-sm bg-secondary hover:bg-secondary/80 rounded-lg transition-colors"
          >
            <RefreshCw size={16} />
            Refresh
          </button>
        </div>
      </div>

      {/* Policy Filter Banner */}
      {selectedPolicy && (
        <div className="flex items-center justify-between bg-primary/10 border border-primary/20 rounded-lg px-4 py-3">
          <div className="flex items-center gap-3">
            <Shield className="h-5 w-5 text-primary" />
            <div>
              <p className="text-sm font-medium">
                Filtered by policy: <span className="text-primary">{selectedPolicy.name}</span>
              </p>
              {selectedPolicy.description && (
                <p className="text-xs text-muted-foreground mt-0.5">{selectedPolicy.description}</p>
              )}
            </div>
          </div>
          <button
            onClick={clearPolicyFilter}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-background hover:bg-muted rounded-md transition-colors"
          >
            <X size={14} />
            Clear filter
          </button>
        </div>
      )}

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

      {/* Cluster List/Grid */}
      {isLoading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </div>
      ) : filteredClusters.length > 0 ? (
        viewMode === "list" ? (
          <ClusterTable clusters={filteredClusters} policyMap={policyMap} />
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {filteredClusters.map((cluster) => (
              <ClusterCard key={cluster.cluster_id} cluster={cluster} />
            ))}
          </div>
        )
      ) : policyFilter && clusters && clusters.length > 0 ? (
        <div className="text-center py-12">
          <Shield className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
          <h2 className="text-lg font-semibold mb-2">No clusters using this policy</h2>
          <p className="text-muted-foreground mb-4">
            No clusters are currently configured with the selected policy.
          </p>
          <button
            onClick={clearPolicyFilter}
            className="inline-flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg"
          >
            <X size={16} />
            Clear filter
          </button>
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
  validateSearch: (search: Record<string, unknown>): ClusterSearchParams => {
    return {
      policy: typeof search.policy === "string" ? search.policy : undefined,
    };
  },
});
