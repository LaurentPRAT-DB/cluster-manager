-- =============================================================================
-- MCP Connection Setup Template for APX Apps
-- =============================================================================
-- This script creates a Unity Catalog HTTP Connection for your APX MCP server.
--
-- PREREQUISITES:
-- 1. APX app deployed with MCP router at /api/mcp
-- 2. OAuth secret created for app's service principal
--
-- USAGE:
-- 1. Replace all <PLACEHOLDER> values
-- 2. Run in Databricks SQL editor
-- =============================================================================

-- =============================================================================
-- Configuration (REPLACE THESE VALUES)
-- =============================================================================

-- Your app URL (from: databricks apps get <app-name>)
-- Example: https://my-app-1234567890.aws.databricksapps.com
SET APP_URL = '<YOUR_APP_URL>';

-- Your workspace URL
-- Example: https://my-workspace.cloud.databricks.com
SET WORKSPACE_URL = '<YOUR_WORKSPACE_URL>';

-- Service principal client ID (from: databricks apps get <app-name> -> service_principal_client_id)
-- Example: 515368c1-a40e-482c-89da-246833cf0f26
SET CLIENT_ID = '<SERVICE_PRINCIPAL_CLIENT_ID>';

-- OAuth secret (from: databricks api post /api/2.0/accounts/servicePrincipals/<SP_ID>/credentials/secrets)
SET CLIENT_SECRET = '<OAUTH_SECRET>';

-- Connection name (customize as needed)
SET CONNECTION_NAME = '<app_name>_mcp';


-- =============================================================================
-- Step 1: Create the MCP Connection
-- =============================================================================

CREATE OR REPLACE CONNECTION ${CONNECTION_NAME} TYPE HTTP
OPTIONS (
  host '${APP_URL}',
  port '443',
  base_path '/api/mcp',
  client_id '${CLIENT_ID}',
  client_secret '${CLIENT_SECRET}',
  oauth_scope 'all-apis',
  token_endpoint '${WORKSPACE_URL}/oidc/v1/token',
  is_mcp_connection 'true'
);


-- =============================================================================
-- Step 2: Verify Connection
-- =============================================================================

DESCRIBE CONNECTION ${CONNECTION_NAME};


-- =============================================================================
-- Step 3: Test MCP Protocol Methods
-- =============================================================================

-- Test initialize
SELECT 'initialize' as test,
       http_request(
         conn => '${CONNECTION_NAME}',
         method => 'POST',
         path => '',
         json => '{"jsonrpc":"2.0","method":"initialize","id":1}'
       ) as response;

-- Test tools/list
SELECT 'tools/list' as test,
       http_request(
         conn => '${CONNECTION_NAME}',
         method => 'POST',
         path => '',
         json => '{"jsonrpc":"2.0","method":"tools/list","id":2}'
       ) as response;


-- =============================================================================
-- Step 4: Grant Access (Optional)
-- =============================================================================

-- Uncomment to grant access to users/groups
-- GRANT USE CONNECTION ON ${CONNECTION_NAME} TO `user@example.com`;
-- GRANT USE CONNECTION ON ${CONNECTION_NAME} TO `data-team`;


-- =============================================================================
-- Cleanup (if needed)
-- =============================================================================

-- DROP CONNECTION ${CONNECTION_NAME};
