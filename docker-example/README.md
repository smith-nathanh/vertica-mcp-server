# Vertica MCP Server Docker Example

This directory contains a Docker setup for testing the Vertica MCP Server with a local Vertica Community Edition instance.

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB of available RAM for Vertica CE

## Quick Start

1. Start the Vertica container:
   ```bash
   docker-compose up -d
   ```

2. Wait for the container to be ready (this may take a few minutes):
   ```bash
   docker-compose logs -f vertica-ce
   ```

3. Test the connection:
   ```bash
   docker exec -it vertica-mcp-test /opt/vertica/bin/vsql -U testuser -d testdb -c "SELECT 1"
   ```

## Configuration

### Environment Variables

Set these environment variables to connect to the Vertica instance:

```bash
export DB_CONNECTION_STRING="vertica://testuser:TestUser123!@localhost:5433/testdb"
```

### Alternative Connection Formats

The server supports multiple connection string formats:

1. **URL Format**: `vertica://username:password@hostname:port/database`
2. **Simple Format**: `hostname:port/database` (uses VERTICA_USER and VERTICA_PASSWORD env vars)

## Sample Data

The setup includes sample tables:
- `testuser.employees` - Employee records
- `testuser.departments` - Department information  
- `testuser.employee_details` - View joining employees and departments

## Testing the MCP Server

Once Vertica is running, you can test the MCP server:

```bash
# Install the server
cd .. && pip install -e .

# Run the server
vertica-mcp-server

# Or run directly
python -m vertica_mcp_server.server
```

## Stopping

To stop and remove the container:

```bash
docker-compose down
```

To also remove the data volume:

```bash
docker-compose down -v
```

## Troubleshooting

- If the container fails to start, check available memory (Vertica CE needs at least 4GB)
- Check logs: `docker-compose logs vertica-ce`
- Connect directly to debug: `docker exec -it vertica-mcp-test /bin/bash`