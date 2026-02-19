# TODO List

## #1: Teams/Slack MCP Integration

**Status:** Pending

Test integration of the Cluster Manager MCP server with Teams or Slack for conversational cluster management.

### 1. Chat Integration
- Send messages in a chat channel that interact with the Databricks managed MCP server
- MCP server receives requests, executes cluster management tools, and responds

### 2. User-Based Context Persistence
- **User Recognition**: Identify user by Teams/Slack user ID
- **Context Storage**: Persist conversation history per user (likely in Unity Catalog or Lakebase)
- **Context Reload**: Automatically reload user's conversation context when they return
- **Fresh Start Option**: Allow user to explicitly start a new conversation ("start fresh", "new conversation")

### 3. Adaptive Session Management
- **Inactivity Detection**: Track time since last interaction per user
- **Smart Summarization**: If gap > threshold, offer to summarize recent conversation before continuing
  - "It's been 4 hours since we last chatted. Would you like a summary of where we left off?"
- **Adaptive Threshold**: Learn user's activity patterns to adjust the summarization prompt delay
  - Track typical session gaps per user
  - Identify user activity trends (e.g., morning user, sporadic user, continuous user)
  - Adjust threshold dynamically based on learned patterns
- **Configurable Override**: Allow users to set their own preferred threshold ("remind me after 2 hours of inactivity")

### 4. Example Interactions
- "Show me all running clusters"
- "What clusters are using the most DBUs?"
- "Stop cluster xyz" (with confirmation)
- "Get events for cluster abc"
- "Summarize our last conversation"
- "Start a new conversation"

### Technical Requirements
- Teams or Slack bot/app setup
- OAuth configuration for MCP server access
- User profile storage (preferences, activity patterns)
- Conversation state management with timestamps
- ML/heuristics for activity trend detection
