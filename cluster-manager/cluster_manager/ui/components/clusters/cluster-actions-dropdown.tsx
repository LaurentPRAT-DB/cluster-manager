import { useNavigate } from "@tanstack/react-router";
import { ExternalLink, FileText, MoreVertical, Settings, Zap } from "lucide-react";
import { useWorkspaceInfo } from "../../lib/api";
import {
  DropdownMenu,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
} from "../ui/dropdown-menu";

interface ClusterActionsDropdownProps {
  clusterId: string;
}

export function ClusterActionsDropdown({ clusterId }: ClusterActionsDropdownProps) {
  const { data: workspaceInfo } = useWorkspaceInfo();
  const navigate = useNavigate();

  // Construct Databricks workspace URLs using the hash-based routing format
  const workspaceHost = workspaceInfo?.host || "";
  // Cluster overview/configuration page
  const clusterUrl = workspaceHost
    ? `${workspaceHost}/#setting/clusters/${clusterId}/configuration`
    : null;
  // Driver logs tab
  const driverLogsUrl = workspaceHost
    ? `${workspaceHost}/#setting/clusters/${clusterId}/driverLogs`
    : null;
  // Spark UI tab
  const sparkUiUrl = workspaceHost
    ? `${workspaceHost}/#setting/clusters/${clusterId}/sparkUi`
    : null;
  // Metrics tab
  const metricsUrl = workspaceHost
    ? `${workspaceHost}/#setting/clusters/${clusterId}/metrics`
    : null;

  const handleViewInApp = () => {
    navigate({ to: "/clusters/$clusterId", params: { clusterId } });
  };

  return (
    <DropdownMenu trigger={<MoreVertical size={16} />}>
      <DropdownMenuLabel>View in App</DropdownMenuLabel>
      <DropdownMenuItem icon={<Settings size={14} />} onClick={handleViewInApp}>
        Cluster Details
      </DropdownMenuItem>

      <DropdownMenuSeparator />
      <DropdownMenuLabel>Open in Databricks</DropdownMenuLabel>

      <DropdownMenuItem
        href={clusterUrl || undefined}
        external
        icon={<Settings size={14} />}
        disabled={!clusterUrl}
      >
        Configuration
      </DropdownMenuItem>

      <DropdownMenuItem
        href={driverLogsUrl || undefined}
        external
        icon={<FileText size={14} />}
        disabled={!driverLogsUrl}
      >
        Driver Logs
      </DropdownMenuItem>

      <DropdownMenuItem
        href={sparkUiUrl || undefined}
        external
        icon={<Zap size={14} />}
        disabled={!sparkUiUrl}
      >
        Spark UI
      </DropdownMenuItem>

      <DropdownMenuItem
        href={metricsUrl || undefined}
        external
        icon={<ExternalLink size={14} />}
        disabled={!metricsUrl}
      >
        Metrics
      </DropdownMenuItem>
    </DropdownMenu>
  );
}
