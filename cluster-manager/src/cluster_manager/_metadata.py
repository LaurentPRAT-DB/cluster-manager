"""Application metadata and constants."""

from pathlib import Path

# Application identity
app_name = "Cluster Manager"
app_slug = "cluster_manager"

# API configuration
api_prefix = "/api"

# Paths
project_root = Path(__file__).parent.parent.parent
dist_dir = project_root / "src" / app_slug / "__dist__"
