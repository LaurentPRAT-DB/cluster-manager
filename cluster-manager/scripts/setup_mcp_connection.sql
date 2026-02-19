-- =============================================================================
-- Cluster Manager MCP Connection Setup
-- =============================================================================
-- This script creates a Unity Catalog HTTP Connection that enables AI agents
-- to interact with the Cluster Manager app via the Model Context Protocol.
--
-- PREREQUISITES:
-- 1. Cluster Manager app deployed and running
-- 2. Service Principal with appropriate permissions
-- 3. Unity Catalog enabled workspace
--
-- INSTRUCTIONS:
-- 1. Replace the placeholder values below with your actual values
-- 2. Run this script in a Databricks SQL editor
-- =============================================================================

-- Configuration (UPDATE THESE VALUES)
-- ------------------------------------
-- Your Cluster Manager app URL (without trailing slash)
-- Example: https://cluster-manager-1444828305810485.aws.databricksapps.com
SET APP_HOST = 'https://cluster-manager-1444828305810485.aws.databricksapps.com';

-- Your Databricks workspace URL
-- Example: https://e2-demo-field-eng.cloud.databricks.com
SET WORKSPACE_URL = 'https://e2-demo-field-eng.cloud.databricks.com';

-- Service Principal credentials for OAuth M2M authentication
-- Get these from your Azure AD / Databricks account console
SET SP_CLIENT_ID = '<YOUR_SERVICE_PRINCIPAL_APPLICATION_ID>';
SET SP_CLIENT_SECRET = '<YOUR_SERVICE_PRINCIPAL_SECRET>';

-- Connection name (can customize)
SET CONNECTION_NAME = 'cluster_manager_mcp';


-- =============================================================================
-- Step 1: Create the MCP Connection
-- =============================================================================

CREATE OR REPLACE CONNECTION ${CONNECTION_NAME} TYPE HTTP
OPTIONS (
  -- App endpoint configuration
  host '${APP_HOST}',
  port '443',
  base_path '/api/mcp',

  -- OAuth Machine-to-Machine (M2M) authentication
  client_id '${SP_CLIENT_ID}',
  client_secret '${SP_CLIENT_SECRET}',
  oauth_scope 'all-apis',
  token_endpoint '${WORKSPACE_URL}/oidc/v1/token',

  -- CRITICAL: This identifies the connection as an MCP server
  is_mcp_connection 'true'
);


-- =============================================================================
-- Step 2: Grant Access (Optional - update with your principals)
-- =============================================================================

-- Uncomment and modify to grant access to specific users/groups
-- GRANT USE CONNECTION ON ${CONNECTION_NAME} TO `user@example.com`;
-- GRANT USE CONNECTION ON ${CONNECTION_NAME} TO `data-team`;


-- =============================================================================
-- Step 3: Test the Connection
-- =============================================================================

-- Test 1: Initialize the MCP protocol
SELECT 'Initialize Test' as test_name,
       http_request(
         conn => '${CONNECTION_NAME}',
         method => 'POST',
         path => '',
         json => '{"jsonrpc":"2.0","method":"initialize","id":1}'
       ) as response;

-- Test 2: List available tools
SELECT 'Tools List Test' as test_name,
       http_request(
         conn => '${CONNECTION_NAME}',
         method => 'POST',
         path => '',
         json => '{"jsonrpc":"2.0","method":"tools/list","id":2}'
       ) as response;

-- Test 3: Call list_clusters tool
SELECT 'List Clusters Test' as test_name,
       http_request(
         conn => '${CONNECTION_NAME}',
         method => 'POST',
         path => '',
         json => '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"list_clusters","arguments":{"limit":5}},"id":3}'
       ) as response;


-- =============================================================================
-- Verification Queries
-- =============================================================================

-- View connection details
DESCRIBE CONNECTION ${CONNECTION_NAME};

-- List all MCP connections in the workspace
SELECT * FROM system.information_schema.connections
WHERE connection_type = 'HTTP'
AND connection_options LIKE '%is_mcp_connection%';


-- =============================================================================
-- Cleanup (if needed)
-- =============================================================================

-- To remove the connection:
-- DROP CONNECTION ${CONNECTION_NAME};
