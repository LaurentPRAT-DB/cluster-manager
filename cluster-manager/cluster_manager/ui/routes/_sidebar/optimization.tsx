import { useState } from "react";
import { createFileRoute, Link } from "@tanstack/react-router";
import {
  AlertTriangle,
  ArrowRight,
  BarChart3,
  Calendar,
  Clock,
  DollarSign,
  Lightbulb,
  Loader2,
  Play,
  RefreshCw,
  Target,
  TrendingDown,
  TrendingUp,
  Users,
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
  useCollectMetrics,
  useJobRecommendations,
  useOptimizationSummary,
  useOversizedClusters,
  useScheduleRecommendations,
  useUtilizationTrends,
} from "@/lib/api";
import { cn, formatCurrency, formatDate, formatNumber } from "@/lib/utils";

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

type TabType = "trends" | "oversized" | "jobs" | "schedule";

function OptimizationPage() {
  const [activeTab, setActiveTab] = useState<TabType>("trends");

  const { data: summary, isLoading: summaryLoading } = useOptimizationSummary();
  const { data: oversizedClusters, isLoading: oversizedLoading } = useOversizedClusters(5);
  const { data: jobRecommendations, isLoading: jobsLoading } = useJobRecommendations();
  const { data: scheduleRecommendations, isLoading: scheduleLoading } = useScheduleRecommendations();
  const { data: trendsData, isLoading: trendsLoading } = useUtilizationTrends(30, 7);

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
            ) : oversizedClusters && oversizedClusters.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <th className="text-left py-3 px-4 font-medium text-sm">Cluster</th>
                      <th className="text-left py-3 px-4 font-medium text-sm">Type</th>
                      <th className="text-center py-3 px-4 font-medium text-sm">Workers</th>
                      <th className="text-center py-3 px-4 font-medium text-sm">Efficiency</th>
                      <th className="text-center py-3 px-4 font-medium text-sm">Recommended</th>
                      <th className="text-right py-3 px-4 font-medium text-sm">Monthly Savings</th>
                      <th className="text-right py-3 px-4 font-medium text-sm"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {oversizedClusters.map((cluster) => (
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
                          <Link
                            to="/clusters/$clusterId"
                            params={{ clusterId: cluster.cluster_id }}
                            className="p-1.5 rounded-md hover:bg-muted transition-colors inline-flex"
                          >
                            <ArrowRight size={16} />
                          </Link>
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

        {activeTab === "jobs" && (
          <div className="p-6">
            <div className="flex items-center gap-2 mb-4">
              <Play className="h-5 w-5 text-blue-500" />
              <h2 className="text-lg font-semibold">Job Cluster Recommendations</h2>
            </div>
            <p className="text-sm text-muted-foreground mb-4">
              Suggestions to consolidate job workloads onto underutilized clusters.
            </p>

            {jobsLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : jobRecommendations && jobRecommendations.length > 0 ? (
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
            <div className="flex items-center gap-2 mb-4">
              <Calendar className="h-5 w-5 text-purple-500" />
              <h2 className="text-lg font-semibold">Schedule Optimization</h2>
            </div>
            <p className="text-sm text-muted-foreground mb-4">
              Recommendations to optimize auto-termination and idle time settings.
            </p>

            {scheduleLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : scheduleRecommendations && scheduleRecommendations.length > 0 ? (
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
