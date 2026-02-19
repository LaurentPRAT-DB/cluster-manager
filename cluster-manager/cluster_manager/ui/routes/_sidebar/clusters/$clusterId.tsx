import { createFileRoute, Link, useRouter } from "@tanstack/react-router";
import {
  AlertCircle,
  ArrowLeft,
  Calendar,
  Clock,
  Cpu,
  ExternalLink,
  Loader2,
  Play,
  RefreshCw,
  Settings,
  Shield,
  Square,
  Tag,
  Users,
} from "lucide-react";
import { toast } from "sonner";

import { useCluster, useClusterEvents, useStartCluster, useStopCluster } from "@/lib/api";
import { cn, formatDateTime, formatDuration, formatNumber } from "@/lib/utils";

const stateColors: Record<string, { bg: string; text: string }> = {
  RUNNING: { bg: "bg-green-100 dark:bg-green-900/30", text: "text-green-700 dark:text-green-400" },
  PENDING: { bg: "bg-yellow-100 dark:bg-yellow-900/30", text: "text-yellow-700 dark:text-yellow-400" },
  RESTARTING: { bg: "bg-blue-100 dark:bg-blue-900/30", text: "text-blue-700 dark:text-blue-400" },
  RESIZING: { bg: "bg-blue-100 dark:bg-blue-900/30", text: "text-blue-700 dark:text-blue-400" },
  TERMINATING: { bg: "bg-orange-100 dark:bg-orange-900/30", text: "text-orange-700 dark:text-orange-400" },
  TERMINATED: { bg: "bg-gray-100 dark:bg-gray-800", text: "text-gray-600 dark:text-gray-400" },
  ERROR: { bg: "bg-red-100 dark:bg-red-900/30", text: "text-red-700 dark:text-red-400" },
};

function InfoRow({
  icon: Icon,
  label,
  value,
  link,
}: {
  icon: React.ElementType;
  label: string;
  value: React.ReactNode;
  link?: string;
}) {
  const content = (
    <div className="flex items-start gap-3 py-2">
      <Icon className="h-4 w-4 text-muted-foreground mt-0.5" />
      <div className="flex-1 min-w-0">
        <p className="text-sm text-muted-foreground">{label}</p>
        <p className="font-medium truncate">{value || "-"}</p>
      </div>
      {link && <ExternalLink className="h-4 w-4 text-muted-foreground" />}
    </div>
  );

  if (link) {
    return (
      <a href={link} target="_blank" rel="noopener noreferrer" className="hover:bg-muted rounded-lg px-2 -mx-2">
        {content}
      </a>
    );
  }

  return <div className="px-2 -mx-2">{content}</div>;
}

function ClusterDetailPage() {
  const { clusterId } = Route.useParams();
  const router = useRouter();
  const { data: cluster, isLoading, error, refetch } = useCluster(clusterId);
  const { data: eventsData } = useClusterEvents(clusterId, 20);
  const startCluster = useStartCluster();
  const stopCluster = useStopCluster();

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  if (error || !cluster) {
    return (
      <div className="flex flex-col items-center justify-center py-12">
        <AlertCircle className="h-12 w-12 text-destructive mb-4" />
        <h2 className="text-lg font-semibold mb-2">Cluster not found</h2>
        <p className="text-muted-foreground mb-4">{error?.message || "The cluster could not be loaded"}</p>
        <Link to="/clusters" className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg">
          <ArrowLeft size={16} />
          Back to Clusters
        </Link>
      </div>
    );
  }

  const isRunning = cluster.state === "RUNNING";
  const isTerminated = cluster.state === "TERMINATED";
  const isTransitioning = ["PENDING", "RESTARTING", "RESIZING", "TERMINATING"].includes(cluster.state);
  const colors = stateColors[cluster.state] || stateColors.TERMINATED;

  const handleStart = () => {
    startCluster.mutate(clusterId, {
      onSuccess: (data) => toast.success(data.message),
      onError: (error) => toast.error(`Failed to start: ${error.message}`),
    });
  };

  const handleStop = () => {
    stopCluster.mutate(clusterId, {
      onSuccess: (data) => toast.success(data.message),
      onError: (error) => toast.error(`Failed to stop: ${error.message}`),
    });
  };

  const workersDisplay = cluster.autoscale
    ? `${cluster.autoscale.min_workers}-${cluster.autoscale.max_workers} (autoscale)`
    : `${cluster.num_workers ?? 0} (fixed)`;

  const allTags = { ...cluster.default_tags, ...cluster.custom_tags };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div className="flex items-center gap-4">
          <button
            onClick={() => router.history.back()}
            className="p-2 hover:bg-muted rounded-lg transition-colors"
            title="Go back"
          >
            <ArrowLeft size={20} />
          </button>
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-2xl font-bold">{cluster.cluster_name}</h1>
              <span className={cn("px-3 py-1 rounded-full text-sm font-medium", colors.bg, colors.text)}>
                {cluster.state}
              </span>
            </div>
            <p className="text-muted-foreground">
              {cluster.cluster_id} &bull; Created by {cluster.creator_user_name || "Unknown"}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => refetch()}
            className="p-2 hover:bg-muted rounded-lg transition-colors"
            title="Refresh"
          >
            <RefreshCw size={18} />
          </button>
        </div>
      </div>

      {/* Action Buttons */}
      <div className="flex gap-3">
        <button
          onClick={handleStart}
          disabled={!isTerminated || startCluster.isPending}
          className={cn(
            "flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-colors",
            isTerminated
              ? "bg-green-600 hover:bg-green-700 text-white"
              : "bg-muted text-muted-foreground cursor-not-allowed"
          )}
        >
          {startCluster.isPending ? <Loader2 size={16} className="animate-spin" /> : <Play size={16} />}
          Start Cluster
        </button>
        <button
          onClick={handleStop}
          disabled={!isRunning || isTransitioning || stopCluster.isPending}
          className={cn(
            "flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-colors",
            isRunning && !isTransitioning
              ? "bg-secondary hover:bg-secondary/80 text-secondary-foreground"
              : "bg-muted text-muted-foreground cursor-not-allowed"
          )}
        >
          {stopCluster.isPending ? <Loader2 size={16} className="animate-spin" /> : <Square size={16} />}
          Stop Cluster
        </button>
      </div>

      {/* State Message */}
      {cluster.state_message && (
        <div className="p-4 bg-muted rounded-lg">
          <p className="text-sm">{cluster.state_message}</p>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Configuration */}
        <div className="lg:col-span-2 space-y-6">
          <div className="bg-card rounded-lg border p-6">
            <h2 className="text-lg font-semibold mb-4">Configuration</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <InfoRow icon={Users} label="Workers" value={workersDisplay} />
              <InfoRow icon={Cpu} label="Node Type" value={cluster.node_type_id} />
              <InfoRow icon={Cpu} label="Driver Type" value={cluster.driver_node_type_id || cluster.node_type_id} />
              <InfoRow icon={Settings} label="Spark Version" value={cluster.spark_version} />
              <InfoRow icon={Shield} label="Data Security Mode" value={cluster.data_security_mode} />
              {cluster.policy_id && (
                <InfoRow
                  icon={Shield}
                  label="Policy"
                  value={cluster.policy_id}
                  link={`/policies/${cluster.policy_id}`}
                />
              )}
            </div>
          </div>

          {/* Timing */}
          <div className="bg-card rounded-lg border p-6">
            <h2 className="text-lg font-semibold mb-4">Timing</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <InfoRow
                icon={Calendar}
                label="Start Time"
                value={cluster.start_time ? formatDateTime(cluster.start_time) : "-"}
              />
              <InfoRow
                icon={Clock}
                label="Uptime"
                value={cluster.uptime_minutes > 0 ? formatDuration(cluster.uptime_minutes) : "-"}
              />
              <InfoRow
                icon={Calendar}
                label="Last Activity"
                value={cluster.last_activity_time ? formatDateTime(cluster.last_activity_time) : "-"}
              />
              {cluster.terminated_time && (
                <InfoRow
                  icon={Calendar}
                  label="Terminated Time"
                  value={formatDateTime(cluster.terminated_time)}
                />
              )}
            </div>
            {cluster.termination_reason && (
              <div className="mt-4 p-3 bg-muted rounded-lg">
                <p className="text-sm text-muted-foreground">Termination Reason</p>
                <p className="text-sm">{cluster.termination_reason}</p>
              </div>
            )}
          </div>

          {/* Tags */}
          {Object.keys(allTags).length > 0 && (
            <div className="bg-card rounded-lg border p-6">
              <h2 className="text-lg font-semibold mb-4">Tags</h2>
              <div className="flex flex-wrap gap-2">
                {Object.entries(allTags).map(([key, value]) => (
                  <span
                    key={key}
                    className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-muted rounded-full text-sm"
                  >
                    <Tag size={12} />
                    <span className="font-medium">{key}:</span>
                    <span className="text-muted-foreground">{value}</span>
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Sidebar */}
        <div className="space-y-6">
          {/* Metrics Card */}
          <div className="bg-card rounded-lg border p-6">
            <h2 className="text-lg font-semibold mb-4">Metrics</h2>
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Est. DBU/hour</span>
                <span className="font-medium">{formatNumber(cluster.estimated_dbu_per_hour)}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Source</span>
                <span className="font-medium">{cluster.cluster_source || "-"}</span>
              </div>
            </div>
          </div>

          {/* Recent Events */}
          <div className="bg-card rounded-lg border p-6">
            <h2 className="text-lg font-semibold mb-4">Recent Events</h2>
            {eventsData?.events && eventsData.events.length > 0 ? (
              <div className="space-y-3">
                {eventsData.events.slice(0, 10).map((event, idx) => (
                  <div key={idx} className="flex items-start gap-3 text-sm">
                    <div className="w-2 h-2 rounded-full bg-primary mt-1.5 flex-shrink-0" />
                    <div className="flex-1 min-w-0">
                      <p className="font-medium">{event.event_type}</p>
                      <p className="text-xs text-muted-foreground">
                        {formatDateTime(event.timestamp)}
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">No recent events</p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export const Route = createFileRoute("/_sidebar/clusters/$clusterId")({
  component: ClusterDetailPage,
});
