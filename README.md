# Vertica MCP Server

A Model Context Protocol (MCP) server that provides comprehensive Vertica Database integration capabilities for AI assistants and agents.

## Features

- **Query Execution**: Execute SQL queries with safety controls (SELECT, DESCRIBE, EXPLAIN only)
- **Schema Inspection**: Browse database tables, views, and Vertica-specific projections
- **Database Documentation**: Generate comprehensive database documentation
- **Query Analysis**: Analyze query performance with execution plans
- **Data Export**: Export query results in JSON and CSV formats
- **Vertica-Specific**: Support for projections, cluster status, and columnar storage features

## Installation

### Prerequisites

- Python 3.11 or higher
- Access to a Vertica database
- `uv` package manager

### Install from Source

```bash
git clone <repository-url>
cd vertica-mcp-server
uv sync
```

### Install as Package

```bash
uv add vertica-mcp-server
```

## Quick Start

1. **Set up environment variables**:
   ```bash
   export DB_CONNECTION_STRING="vertica://username:password@hostname:5433/database"
   ```

2. **Start the MCP server**:
   ```bash
   vertica-mcp-server
   ```

3. **Use with AI assistants** that support MCP (like Claude Desktop or VS Code extensions)

## Configuration

### Connection String Formats

**URL Format** (recommended):
```
vertica://username:password@hostname:port/database
```

**Simple Format**:
```
hostname:port/database
```
When using simple format, set these environment variables:
- `VERTICA_USER`: Database username
- `VERTICA_PASSWORD`: Database password

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_CONNECTION_STRING` | Database connection string | Required |
| `VERTICA_USER` | Database username (for simple format) | `dbadmin` |
| `VERTICA_PASSWORD` | Database password (for simple format) | Empty |
| `QUERY_LIMIT_SIZE` | Maximum rows returned per query | `100` |
| `MAX_ROWS_EXPORT` | Maximum rows for export operations | `10000` |
| `DEBUG` | Enable debug logging | `false` |
| `TABLE_WHITE_LIST` | Comma-separated list of allowed tables | All tables |
| `COLUMN_WHITE_LIST` | Comma-separated list of allowed columns | All columns |

## Docker Development

A Docker setup is provided for testing with Vertica Community Edition:

```bash
cd docker-example
docker-compose up -d
```

See [docker-example/README.md](docker-example/README.md) for detailed instructions.

## Available Tools

The MCP server provides these tools for AI assistants:

- **execute_query**: Execute SQL queries (SELECT, DESCRIBE, EXPLAIN only)
- **describe_table**: Get detailed table column information
- **list_tables**: List all tables with metadata
- **list_views**: List all database views
- **list_projections**: List Vertica projections (unique to Vertica)
- **explain_query**: Get query execution plans
- **generate_sample_queries**: Generate example queries for tables
- **export_query_results**: Export query results as JSON or CSV

## Vertica-Specific Features

This server includes Vertica-specific functionality:

- **Projections**: List and inspect Vertica projections
- **Columnar Storage**: Handle Vertica's columnar data types
- **Query Optimization**: Vertica-specific query execution plans
- **Data Types**: Support for Vertica data types (UUID, ARRAY, etc.)

## Safety Features

- **Query Restriction**: Only SELECT, DESCRIBE, and EXPLAIN statements allowed
- **Row Limiting**: Automatic LIMIT clause added to prevent large result sets
- **SQL Injection Protection**: Basic protection against malicious queries
- **Whitelisting**: Optional table and column whitelisting

## Development

### Running Tests

```bash
# Run all tests
uv run pytest

# Run unit tests only
uv run pytest -m unit

# Run integration tests only (requires running Vertica instance)
uv run pytest -m integration

# Run tests with coverage
uv run pytest --cov=src/vertica_mcp_server
```

### Code Quality

```bash
# Format code
uv run black src/ tests/

# Sort imports
uv run isort src/ tests/

# Type checking
uv run mypy src/

# Lint code
uv run flake8 src/ tests/
```

### Project Structure

```
vertica-mcp-server/
   src/vertica_mcp_server/
      __init__.py
      server.py          # Main server implementation
      py.typed
   tests/                 # Test suite
   docker-example/        # Docker setup for testing
   docs/                  # Documentation
   pyproject.toml        # Project configuration
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Changelog

### 0.1.0
- Initial release
- Core MCP server functionality
- Vertica database integration
- Query execution and schema inspection
- Docker development environment
- Comprehensive test suite