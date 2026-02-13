---
name: bkend-quickstart
description: bkend.ai platform onboarding and core concepts guide.
---


# bkend.ai Quick Start Guide

## What is bkend.ai

MCP-based BaaS platform providing Database, Authentication, and Storage services.
Manage backend via natural language from AI tools (Claude Code, Cursor).

## Resource Hierarchy

```
Organization (team/billing) -> Project (service unit) -> Environment (dev/staging/prod, data isolation)
```

## Tenant vs User

- **Tenant**: Service builder (OAuth 2.1 auth, MCP/Management API access)
- **User**: App end-user (JWT auth, Service API access)
- One person can have both roles

## MCP Setup (Claude Code)

### Quick Setup (One Command)

```bash
claude mcp add bkend --transport http https://api.bkend.ai/mcp
```

### Step-by-Step Guide

1. **Prerequisites**: bkend.ai account (signup at https://console.bkend.ai)
2. **Run setup command**: `claude mcp add bkend --transport http https://api.bkend.ai/mcp`
3. **OAuth authentication**: Browser auto-opens for OAuth 2.1 + PKCE auth (no API key needed)
4. **Verify connection**: Ask "Show my connected bkend projects" or use `0_get_context` MCP tool
5. **Create .mcp.json** (optional, for team sharing):
   ```json
   {
     "mcpServers": {
       "bkend": {
         "type": "http",
         "url": "https://api.bkend.ai/mcp"
       }
     }
   }
   ```

### Troubleshooting MCP Connection

| Problem | Solution |
|---------|----------|
| OAuth popup not appearing | Check browser popup blocker |
| MCP tools not visible | Run `claude mcp list` to verify, re-add if needed |
| Connection lost | Re-authenticate (automatic on next MCP call) |
| Wrong project/env | Use `0_get_context` to check current session |

## MCP Guide Tools (No Parameters)

| Tool | Purpose |
|------|---------|
| 0_get_context | Session context (org/project/env) |
| 1_concepts | Core concepts (BSON, permissions, hierarchy) |
| 2_tutorial | Project~table creation tutorial |

## First Project Checklist

1. Sign up at bkend.ai -> Create Organization
2. Create Project -> dev environment auto-created
3. Connect MCP -> `claude mcp add bkend`
4. Create first table -> "Create a users table"
5. Start data operations -> CRUD via natural language

## Console URL

```
https://console.bkend.ai
```

## Next Steps

- Database operations: refer to bkend-data skill
- Authentication: refer to bkend-auth skill
- File storage: refer to bkend-storage skill
- Practical tutorials: refer to bkend-cookbook skill

## Official Documentation (Live Reference)

For the latest bkend documentation, use WebFetch:
- Full TOC: https://raw.githubusercontent.com/popup-studio-ai/bkend-docs/main/SUMMARY.md
- Getting Started: https://raw.githubusercontent.com/popup-studio-ai/bkend-docs/main/src/getting-started/
- AI Tools/MCP: https://raw.githubusercontent.com/popup-studio-ai/bkend-docs/main/src/ai-tools/
- Console: https://raw.githubusercontent.com/popup-studio-ai/bkend-docs/main/src/console/

---

## Claude Code 원본 참조 정보

- **원본 에이전트**: bkit:bkend-expert
- **사용자 호출 가능**: false
- **임포트**: ./templates/shared/bkend-patterns.md
