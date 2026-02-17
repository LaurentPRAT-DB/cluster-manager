import { createFileRoute, Link } from "@tanstack/react-router";
import {
  AlertCircle,
  AlertTriangle,
  BarChart3,
  DollarSign,
  Lightbulb,
  Loader2,
  RefreshCw,
  TrendingUp,
  Zap,
} from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  useBillingSummary,
  useBillingTrend,
  useIdleClusters,
  useRecommendations,
  useTopConsumers,
} from "@/lib/api";
import { cn, formatCurrency, formatDate, formatDuration, formatNumber } from "@/lib/utils";

function MetricCard({
  title,
  value,
  icon: Icon,
  subtitle,
  trend,
}: {
  title: string;
  value: string | number;
  icon: React.ElementType;
  subtitle?: string;
  trend?: "up" | "down" | "neutral";
}) {
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
        <div
          className={cn(
            "p-3 rounded-lg",
            trend === "up"
              ? "bg-red-100 dark:bg-red-900/30 text-red-600"
              : trend === "down"
                ? "bg-green-100 dark:bg-green-900/30 text-green-600"
                : "bg-primary/10 text-primary"
          )}
        >
          <Icon className="h-6 w-6" />
        </div>
      </div>
    </div>
  );
}

function AnalyticsPage() {
  const { data: billingSummary, isLoading: billingLoading, error: billingError } = useBillingSummary(30);
  const { data: trend, isLoading: trendLoading } = useBillingTrend(30);
  const { data: topConsumers, isLoading: consumersLoading } = useTopConsumers(30, 10);
  const { data: idleClusters } = useIdleClusters();
  const { data: recommendations } = useRecommendations();

  const chartData = trend?.map((d) => ({
    date: formatDate(d.date),
    dbu: d.dbu,
    cost: d.estimated_cost_usd,
  })) || [];

  const consumerData = topConsumers?.slice(0, 5).map((c) => ({
    name: c.cluster_name || c.cluster_id.slice(0, 8),
    dbu: c.total_dbu,
    cost: c.estimated_cost_usd,
  })) || [];

  if (billingError) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold">Analytics</h1>
          <p className="text-muted-foreground">Cost analysis and optimization insights</p>
        </div>
        <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-6">
          <div className="flex items-start gap-4">
            <AlertCircle className="h-6 w-6 text-destructive flex-shrink-0" />
            <div>
              <h3 className="font-semibold text-destructive">Unable to load billing data</h3>
              <p className="text-sm text-muted-foreground mt-1">
                {billingError.message}
              </p>
              <p className="text-sm text-muted-foreground mt-2">
                Make sure you have access to <code>system.billing.usage</code> table
                and a SQL warehouse is available.
              </p>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Analytics</h1>
          <p className="text-muted-foreground">Cost analysis and optimization insights (Last 30 days)</p>
        </div>
        <button
          onClick={() => window.location.reload()}
          className="flex items-center gap-2 px-3 py-2 text-sm bg-secondary hover:bg-secondary/80 rounded-lg transition-colors"
        >
          <RefreshCw size={16} />
          Refresh
        </button>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {billingLoading ? (
          Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="bg-card rounded-lg border p-5 animate-pulse">
              <div className="h-4 bg-muted rounded w-24 mb-2" />
              <div className="h-8 bg-muted rounded w-32" />
            </div>
          ))
        ) : billingSummary ? (
          <>
            <MetricCard
              title="Total DBU Usage"
              value={formatNumber(billingSummary.total_dbu)}
              icon={Zap}
              subtitle="Last 30 days"
            />
            <MetricCard
              title="Estimated Cost"
              value={formatCurrency(billingSummary.estimated_cost_usd)}
              icon={DollarSign}
              subtitle="Approximate"
            />
            <MetricCard
              title="Idle Clusters"
              value={idleClusters?.length || 0}
              icon={AlertTriangle}
              subtitle="Currently running"
              trend={idleClusters && idleClusters.length > 0 ? "up" : "neutral"}
            />
            <MetricCard
              title="Recommendations"
              value={recommendations?.length || 0}
              icon={Lightbulb}
              subtitle="Optimization opportunities"
            />
          </>
        ) : null}
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* DBU Trend Chart */}
        <div className="bg-card rounded-lg border p-6">
          <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <TrendingUp size={20} />
            DBU Usage Trend
          </h2>
          {trendLoading ? (
            <div className="h-64 flex items-center justify-center">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
            </div>
          ) : chartData.length > 0 ? (
            <ResponsiveContainer width="100%" height={260}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis
                  dataKey="date"
                  tick={{ fontSize: 12 }}
                  tickLine={false}
                  axisLine={false}
                />
                <YAxis
                  tick={{ fontSize: 12 }}
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
                          <p className="text-sm text-primary">
                            {formatNumber(payload[0].value as number)} DBU
                          </p>
                          <p className="text-sm text-muted-foreground">
                            {formatCurrency(payload[0].payload.cost)}
                          </p>
                        </div>
                      );
                    }
                    return null;
                  }}
                />
                <Line
                  type="monotone"
                  dataKey="dbu"
                  stroke="hsl(var(--primary))"
                  strokeWidth={2}
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-64 flex items-center justify-center text-muted-foreground">
              No data available
            </div>
          )}
        </div>

        {/* Top Consumers Chart */}
        <div className="bg-card rounded-lg border p-6">
          <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <BarChart3 size={20} />
            Top Consumers
          </h2>
          {consumersLoading ? (
            <div className="h-64 flex items-center justify-center">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
            </div>
          ) : consumerData.length > 0 ? (
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={consumerData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" horizontal={false} />
                <XAxis type="number" tickFormatter={(v) => formatNumber(v, 0)} />
                <YAxis
                  type="category"
                  dataKey="name"
                  width={100}
                  tick={{ fontSize: 12 }}
                  tickLine={false}
                  axisLine={false}
                />
                <Tooltip
                  content={({ active, payload }) => {
                    if (active && payload && payload.length) {
                      return (
                        <div className="bg-popover border rounded-lg p-3 shadow-lg">
                          <p className="font-medium">{payload[0].payload.name}</p>
                          <p className="text-sm text-primary">
                            {formatNumber(payload[0].value as number)} DBU
                          </p>
                          <p className="text-sm text-muted-foreground">
                            {formatCurrency(payload[0].payload.cost)}
                          </p>
                        </div>
                      );
                    }
                    return null;
                  }}
                />
                <Bar dataKey="dbu" fill="hsl(var(--primary))" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-64 flex items-center justify-center text-muted-foreground">
              No data available
            </div>
          )}
        </div>
      </div>

      {/* Idle Clusters & Recommendations */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Idle Clusters */}
        <div className="bg-card rounded-lg border p-6">
          <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <AlertTriangle size={20} className="text-yellow-500" />
            Idle Clusters
          </h2>
          {idleClusters && idleClusters.length > 0 ? (
            <div className="space-y-3">
              {idleClusters.map((cluster) => (
                <Link
                  key={cluster.cluster_id}
                  to="/clusters/$clusterId"
                  params={{ clusterId: cluster.cluster_id }}
                  className="block p-3 bg-muted/50 hover:bg-muted rounded-lg transition-colors"
                >
                  <div className="flex items-start justify-between">
                    <div>
                      <p className="font-medium">{cluster.cluster_name}</p>
                      <p className="text-sm text-muted-foreground mt-0.5">
                        Idle for {formatDuration(cluster.idle_duration_minutes)}
                      </p>
                    </div>
                    <div className="text-right">
                      <p className="text-sm font-medium text-yellow-600">
                        ~{formatNumber(cluster.estimated_wasted_dbu)} DBU wasted
                      </p>
                    </div>
                  </div>
                  <p className="text-sm text-muted-foreground mt-2">{cluster.recommendation}</p>
                </Link>
              ))}
            </div>
          ) : (
            <p className="text-muted-foreground text-center py-8">
              No idle clusters detected
            </p>
          )}
        </div>

        {/* Recommendations */}
        <div className="bg-card rounded-lg border p-6">
          <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <Lightbulb size={20} className="text-blue-500" />
            Optimization Recommendations
          </h2>
          {recommendations && recommendations.length > 0 ? (
            <div className="space-y-3">
              {recommendations.slice(0, 5).map((rec, idx) => (
                <Link
                  key={idx}
                  to="/clusters/$clusterId"
                  params={{ clusterId: rec.cluster_id }}
                  className="block p-3 bg-muted/50 hover:bg-muted rounded-lg transition-colors"
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <p className="font-medium">{rec.cluster_name}</p>
                      <p className="text-sm text-destructive mt-0.5">{rec.issue}</p>
                    </div>
                    <span
                      className={cn(
                        "text-xs px-2 py-0.5 rounded-full",
                        rec.priority === "high"
                          ? "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400"
                          : rec.priority === "medium"
                            ? "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400"
                            : "bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-400"
                      )}
                    >
                      {rec.priority}
                    </span>
                  </div>
                  <p className="text-sm text-muted-foreground mt-2">{rec.recommendation}</p>
                  <p className="text-xs text-green-600 mt-1">Potential savings: {rec.potential_savings}</p>
                </Link>
              ))}
            </div>
          ) : (
            <p className="text-muted-foreground text-center py-8">
              No recommendations at this time
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

export const Route = createFileRoute("/_sidebar/analytics")({
  component: AnalyticsPage,
});
