#!/usr/bin/env python3
"""
Vertica Database MCP Server

This Model Context Protocol server provides comprehensive Vertica Database
interaction capabilities, optimized for use with GitHub Copilot's agentic workflows.

Features:
- Execute SQL queries with safety controls
- Browse database schema (tables, views, projections)
- Generate database documentation
- Analyze query performance
- Export query results
- Database health monitoring
- Vertica-specific features (projections, cluster status)
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import vertica_python
from mcp import stdio_server
from mcp.server import Server
from mcp.server.lowlevel import NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.types import Resource, TextContent, Tool
from pydantic import AnyUrl

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("vertica-mcp-server")

# Configuration from environment variables
DEBUG = os.getenv("DEBUG", "False").lower() == "true"
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
COMMENT_DB_CONNECTION_STRING = os.getenv(
    "COMMENT_DB_CONNECTION_STRING", DB_CONNECTION_STRING
)
TABLE_WHITE_LIST = (
    os.getenv("TABLE_WHITE_LIST", "").split(",")
    if os.getenv("TABLE_WHITE_LIST")
    else []
)
COLUMN_WHITE_LIST = (
    os.getenv("COLUMN_WHITE_LIST", "").split(",")
    if os.getenv("COLUMN_WHITE_LIST")
    else []
)
QUERY_LIMIT_SIZE = int(os.getenv("QUERY_LIMIT_SIZE") or "100")
MAX_ROWS_EXPORT = int(os.getenv("MAX_ROWS_EXPORT") or "10000")

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)


class VerticaConnection:
    """Manages Vertica database connections with connection pooling"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection_info = self._parse_connection_string(connection_string)

    def _parse_connection_string(self, connection_string: str) -> Dict[str, Any]:
        """Parse Vertica connection string into connection parameters"""
        if not connection_string:
            raise ValueError("Database connection string is required")

        # Parse URL format: vertica://username:password@hostname:port/database
        if connection_string.startswith("vertica://"):
            parsed = urlparse(connection_string)
            return {
                "host": parsed.hostname,
                "port": parsed.port or 5433,
                "user": parsed.username,
                "password": parsed.password,
                "database": parsed.path.lstrip("/") if parsed.path else None,
            }
        else:
            # Simple format: host:port/database or host/database
            parts = connection_string.split("/")
            if len(parts) >= 2:
                host_port = parts[0]
                database = parts[1]

                if ":" in host_port:
                    host, port = host_port.split(":", 1)
                    port = int(port)
                else:
                    host = host_port
                    port = 5433

                return {
                    "host": host,
                    "port": port,
                    "database": database,
                    "user": os.getenv("VERTICA_USER", "dbadmin"),
                    "password": os.getenv("VERTICA_PASSWORD", ""),
                }
            else:
                raise ValueError("Invalid connection string format")

    async def get_connection(self) -> vertica_python.Connection:
        """Get a connection to Vertica"""
        try:
            connection = vertica_python.connect(**self.connection_info)
            logger.debug("Vertica connection established successfully")
            return connection
        except Exception as e:
            logger.error(f"Failed to connect to Vertica: {e}")
            raise


class DatabaseInspector:
    """Provides database schema inspection capabilities"""

    def __init__(self, connection_manager: VerticaConnection):
        self.connection_manager = connection_manager

    async def get_tables(
        self, schema_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get list of tables with metadata"""
        conn = await self.connection_manager.get_connection()
        try:
            cursor = conn.cursor()

            # Base query for tables using Vertica system tables
            query = """
                SELECT 
                    t.table_schema,
                    t.table_name,
                    CASE 
                        WHEN t.is_temp_table = 't' THEN 'TEMPORARY TABLE'
                        WHEN t.is_flextable = 't' THEN 'FLEX TABLE'
                        ELSE 'TABLE'
                    END as table_type,
                    t.is_temp_table,
                    t.is_system_table,
                    COALESCE(ps.row_count, 0) as estimated_row_count
                FROM v_catalog.tables t
                LEFT JOIN v_monitor.projection_storage ps ON t.table_name = ps.anchor_table_name
                WHERE t.is_system_table = 'f'
            """

            params = []

            # Filter by schema if specified
            if schema_name:
                query += " AND t.table_schema = %s"
                params.append(schema_name)

            # Apply whitelist filter if configured
            if TABLE_WHITE_LIST and TABLE_WHITE_LIST != [""]:
                placeholders = ",".join(["%s"] * len(TABLE_WHITE_LIST))
                query += f" AND t.table_name IN ({placeholders})"
                params.extend(TABLE_WHITE_LIST)

            query += " ORDER BY t.table_schema, t.table_name"

            cursor.execute(query, params)

            tables = []
            for row in cursor.fetchall():
                tables.append(
                    {
                        "schema_name": row[0],
                        "table_name": row[1],
                        "table_type": row[2],
                        "is_temp_table": row[3],
                        "is_system_table": row[4],
                        "estimated_row_count": row[5],
                    }
                )

            return tables

        finally:
            conn.close()

    async def get_table_columns(
        self, table_name: str, schema_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get detailed column information for a table"""
        conn = await self.connection_manager.get_connection()
        try:
            cursor = conn.cursor()

            query = """
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.data_type_id,
                    c.data_type_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.is_nullable,
                    c.column_default,
                    c.column_set_using,
                    c.ordinal_position
                FROM v_catalog.columns c
                WHERE c.table_name = %s
            """

            params = [table_name]

            if schema_name:
                query += " AND c.table_schema = %s"
                params.append(schema_name)

            query += " ORDER BY c.ordinal_position"

            cursor.execute(query, params)

            columns = []
            for row in cursor.fetchall():
                # Apply column whitelist if configured
                full_column_name = f"{table_name}.{row[0]}"
                if COLUMN_WHITE_LIST and COLUMN_WHITE_LIST != [""]:
                    if full_column_name not in COLUMN_WHITE_LIST:
                        continue

                columns.append(
                    {
                        "column_name": row[0],
                        "data_type": row[1],
                        "data_type_id": row[2],
                        "data_type_length": row[3],
                        "numeric_precision": row[4],
                        "numeric_scale": row[5],
                        "is_nullable": row[6],
                        "column_default": row[7],
                        "column_set_using": row[8],
                        "ordinal_position": row[9],
                    }
                )

            return columns

        finally:
            conn.close()

    async def get_views(
        self, schema_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get list of views"""
        conn = await self.connection_manager.get_connection()
        try:
            cursor = conn.cursor()

            query = """
                SELECT 
                    v.table_schema,
                    v.table_name,
                    v.is_system_view
                FROM v_catalog.views v
                WHERE v.is_system_view = 'f'
            """

            params = []

            if schema_name:
                query += " AND v.table_schema = %s"
                params.append(schema_name)

            query += " ORDER BY v.table_schema, v.table_name"

            cursor.execute(query, params)

            views = []
            for row in cursor.fetchall():
                views.append(
                    {
                        "schema_name": row[0],
                        "view_name": row[1],
                        "is_system_view": row[2],
                    }
                )

            return views

        finally:
            conn.close()

    async def get_projections(
        self, schema_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get list of projections (Vertica-specific)"""
        conn = await self.connection_manager.get_connection()
        try:
            cursor = conn.cursor()

            query = """
                SELECT 
                    p.projection_schema,
                    p.projection_name,
                    p.anchor_table_name,
                    p.is_super_projection,
                    p.is_up_to_date,
                    p.has_statistics,
                    p.created_epoch,
                    p.verified_fault_tolerance
                FROM v_catalog.projections p
                WHERE p.owner_name != 'release'
                  AND p.projection_schema NOT IN ('v_catalog', 'v_monitor', 'v_internal')
            """

            params = []

            if schema_name:
                query += " AND p.projection_schema = %s"
                params.append(schema_name)

            query += " ORDER BY p.projection_schema, p.projection_name"

            cursor.execute(query, params)

            projections = []
            for row in cursor.fetchall():
                projections.append(
                    {
                        "schema_name": row[0],
                        "projection_name": row[1],
                        "anchor_table_name": row[2],
                        "is_super_projection": row[3],
                        "is_up_to_date": row[4],
                        "has_statistics": row[5],
                        "created_epoch": row[6],
                        "verified_fault_tolerance": row[7],
                    }
                )

            return projections

        finally:
            conn.close()


class QueryExecutor:
    """Handles SQL query execution with safety controls"""

    def __init__(self, connection_manager: VerticaConnection):
        self.connection_manager = connection_manager

    async def execute_query(
        self, sql: str, params: Optional[List] = None
    ) -> Dict[str, Any]:
        """Execute a SQL query with safety controls"""

        # Basic SQL injection prevention
        sql_upper = sql.upper().strip()

        # Check for potentially dangerous operations
        dangerous_keywords = [
            "DROP",
            "DELETE",
            "TRUNCATE",
            "ALTER",
            "CREATE",
            "INSERT",
            "UPDATE",
        ]

        # Allow SELECT, DESCRIBE, EXPLAIN
        if not any(
            sql_upper.startswith(keyword)
            for keyword in ["SELECT", "WITH", "DESCRIBE", "DESC", "EXPLAIN"]
        ):
            if any(keyword in sql_upper for keyword in dangerous_keywords):
                raise ValueError(
                    "Only SELECT, DESCRIBE, and EXPLAIN statements are allowed"
                )

        conn = await self.connection_manager.get_connection()
        try:
            cursor = conn.cursor()

            # Set row limit for SELECT queries
            if (
                "SELECT" in sql_upper
                and "LIMIT" not in sql_upper
                and not sql_upper.startswith("EXPLAIN")
            ):
                # Add LIMIT for SELECT queries
                sql += f" LIMIT {QUERY_LIMIT_SIZE}"

            start_time = datetime.now()

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            execution_time = (datetime.now() - start_time).total_seconds()

            # Fetch results
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                # Convert to JSON-serializable format
                serializable_rows = []
                for row in rows:
                    serializable_row = []
                    for value in row:
                        if isinstance(value, datetime):
                            serializable_row.append(value.isoformat())
                        else:
                            serializable_row.append(value)
                    serializable_rows.append(serializable_row)

                return {
                    "columns": columns,
                    "rows": serializable_rows,
                    "row_count": len(rows),
                    "execution_time_seconds": execution_time,
                    "query": sql,
                }
            else:
                return {
                    "message": "Query executed successfully",
                    "execution_time_seconds": execution_time,
                    "query": sql,
                }

        finally:
            conn.close()

    async def explain_query(self, sql: str) -> Dict[str, Any]:
        """Get execution plan for a query"""
        conn = await self.connection_manager.get_connection()
        try:
            cursor = conn.cursor()

            # Use Vertica's EXPLAIN syntax
            explain_sql = f"EXPLAIN {sql}"
            cursor.execute(explain_sql)

            plan_rows = []
            for row in cursor.fetchall():
                plan_rows.append({"plan_line": row[0]})

            return {"execution_plan": plan_rows}

        finally:
            conn.close()


class VerticaMCPServer:
    """Main MCP Server class for Vertica Database integration"""

    def __init__(self):
        self.server = Server("vertica-database")
        self.connection_manager = VerticaConnection(DB_CONNECTION_STRING)
        self.inspector = DatabaseInspector(self.connection_manager)
        self.executor = QueryExecutor(self.connection_manager)

    async def setup_handlers(self):
        """Setup MCP server handlers"""

        @self.server.list_resources()
        async def handle_list_resources() -> list[Resource]:
            """List available database resources"""
            resources = []

            try:
                # Get database schema information
                tables = await self.inspector.get_tables()

                # Add schema overview resource
                resources.append(
                    Resource(
                        uri=AnyUrl("vertica://schema/overview"),
                        name="Database Schema Overview",
                        description="Complete overview of database tables, views, and projections",
                        mimeType="application/json",
                    )
                )

                # Add individual table resources
                for table in tables[:50]:  # Limit to first 50 tables
                    table_uri = (
                        f"vertica://table/{table['schema_name']}.{table['table_name']}"
                    )
                    resources.append(
                        Resource(
                            uri=AnyUrl(table_uri),
                            name=f"Table: {table['schema_name']}.{table['table_name']}",
                            description=f"Schema and metadata for table {table['table_name']}",
                            mimeType="application/json",
                        )
                    )

            except Exception as e:
                logger.error(f"Error listing resources: {e}")

            return resources

        @self.server.read_resource()
        async def handle_read_resource(uri: AnyUrl) -> str:
            """Read a specific database resource"""

            uri_str = str(uri)

            try:
                if uri_str == "vertica://schema/overview":
                    # Return complete schema overview
                    tables = await self.inspector.get_tables()
                    views = await self.inspector.get_views()
                    projections = await self.inspector.get_projections()

                    overview = {
                        "database_type": "Vertica",
                        "tables": tables,
                        "views": views,
                        "projections": projections,
                        "table_count": len(tables),
                        "view_count": len(views),
                        "projection_count": len(projections),
                        "generated_at": datetime.now().isoformat(),
                    }

                    return json.dumps(overview, indent=2, default=str)

                elif uri_str.startswith("vertica://table/"):
                    # Return specific table information
                    table_path = uri_str.replace("vertica://table/", "")

                    if "." in table_path:
                        schema_name, table_name = table_path.split(".", 1)
                    else:
                        schema_name = None
                        table_name = table_path

                    columns = await self.inspector.get_table_columns(
                        table_name, schema_name
                    )

                    table_info = {
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "columns": columns,
                        "column_count": len(columns),
                        "generated_at": datetime.now().isoformat(),
                    }

                    return json.dumps(table_info, indent=2, default=str)

                else:
                    raise ValueError(f"Unknown resource URI: {uri_str}")

            except Exception as e:
                logger.error(f"Error reading resource {uri_str}: {e}")
                raise

        @self.server.list_tools()
        async def handle_list_tools() -> list[Tool]:
            """List available database tools"""

            return [
                Tool(
                    name="execute_query",
                    description="Execute a SQL query against the Vertica database. Only SELECT, DESCRIBE, and EXPLAIN statements are allowed for safety.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "sql": {
                                "type": "string",
                                "description": "SQL query to execute (SELECT, DESCRIBE, or EXPLAIN only)",
                            },
                            "params": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Optional parameters for parameterized queries",
                                "default": [],
                            },
                        },
                        "required": ["sql"],
                    },
                ),
                Tool(
                    name="describe_table",
                    description="Get detailed information about a table including columns, data types, and constraints",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table to describe",
                            },
                            "schema_name": {
                                "type": "string",
                                "description": "Schema name (optional)",
                                "default": None,
                            },
                        },
                        "required": ["table_name"],
                    },
                ),
                Tool(
                    name="list_tables",
                    description="List all tables in the database with metadata",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "schema_name": {
                                "type": "string",
                                "description": "Filter by schema name (optional)",
                                "default": None,
                            }
                        },
                    },
                ),
                Tool(
                    name="list_views",
                    description="List all views in the database",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "schema_name": {
                                "type": "string",
                                "description": "Filter by schema name (optional)",
                                "default": None,
                            }
                        },
                    },
                ),
                Tool(
                    name="list_projections",
                    description="List all projections in the database (Vertica-specific)",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "schema_name": {
                                "type": "string",
                                "description": "Filter by schema name (optional)",
                                "default": None,
                            }
                        },
                    },
                ),
                Tool(
                    name="explain_query",
                    description="Get the execution plan for a SQL query to analyze performance",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "sql": {
                                "type": "string",
                                "description": "SQL query to explain",
                            }
                        },
                        "required": ["sql"],
                    },
                ),
                Tool(
                    name="generate_sample_queries",
                    description="Generate sample SQL queries for a given table to help with exploration",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table to generate queries for",
                            },
                            "schema_name": {
                                "type": "string",
                                "description": "Schema name (optional)",
                                "default": None,
                            },
                        },
                        "required": ["table_name"],
                    },
                ),
                Tool(
                    name="export_query_results",
                    description="Export query results in various formats (JSON, CSV)",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "sql": {
                                "type": "string",
                                "description": "SQL query to execute and export",
                            },
                            "format": {
                                "type": "string",
                                "enum": ["json", "csv"],
                                "description": "Export format",
                                "default": "json",
                            },
                        },
                        "required": ["sql"],
                    },
                ),
            ]

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: dict) -> list[TextContent]:
            """Handle tool calls"""

            try:
                if name == "execute_query":
                    sql = arguments.get("sql")
                    params = arguments.get("params", [])

                    result = await self.executor.execute_query(sql, params)

                    return [
                        TextContent(
                            type="text", text=json.dumps(result, indent=2, default=str)
                        )
                    ]

                elif name == "describe_table":
                    table_name = arguments.get("table_name")
                    schema_name = arguments.get("schema_name")

                    columns = await self.inspector.get_table_columns(
                        table_name, schema_name
                    )

                    result = {
                        "table_name": table_name,
                        "schema_name": schema_name,
                        "columns": columns,
                        "column_count": len(columns),
                    }

                    return [
                        TextContent(
                            type="text", text=json.dumps(result, indent=2, default=str)
                        )
                    ]

                elif name == "list_tables":
                    schema_name = arguments.get("schema_name")
                    tables = await self.inspector.get_tables(schema_name)

                    return [
                        TextContent(
                            type="text",
                            text=json.dumps({"tables": tables}, indent=2, default=str),
                        )
                    ]

                elif name == "list_views":
                    schema_name = arguments.get("schema_name")
                    views = await self.inspector.get_views(schema_name)

                    return [
                        TextContent(
                            type="text",
                            text=json.dumps({"views": views}, indent=2, default=str),
                        )
                    ]

                elif name == "list_projections":
                    schema_name = arguments.get("schema_name")
                    projections = await self.inspector.get_projections(schema_name)

                    return [
                        TextContent(
                            type="text",
                            text=json.dumps(
                                {"projections": projections}, indent=2, default=str
                            ),
                        )
                    ]

                elif name == "explain_query":
                    sql = arguments.get("sql")
                    result = await self.executor.explain_query(sql)

                    return [
                        TextContent(
                            type="text", text=json.dumps(result, indent=2, default=str)
                        )
                    ]

                elif name == "generate_sample_queries":
                    table_name = arguments.get("table_name")
                    schema_name = arguments.get("schema_name")

                    columns = await self.inspector.get_table_columns(
                        table_name, schema_name
                    )

                    # Generate sample queries
                    table_ref = (
                        f"{schema_name}.{table_name}" if schema_name else table_name
                    )

                    queries = [
                        f"-- Basic select all\nSELECT * FROM {table_ref} LIMIT 10;",
                        f"-- Count total rows\nSELECT COUNT(*) FROM {table_ref};",
                    ]

                    # Add column-specific queries
                    for col in columns[:5]:  # Limit to first 5 columns
                        col_name = col["column_name"]
                        data_type = col["data_type"]

                        if data_type in ["VARCHAR", "CHAR", "LONG VARCHAR"]:
                            queries.append(
                                f"-- Find distinct values for {col_name}\nSELECT DISTINCT {col_name} FROM {table_ref} WHERE {col_name} IS NOT NULL LIMIT 20;"
                            )
                        elif data_type in ["INT", "INTEGER", "NUMERIC", "FLOAT"]:
                            queries.append(
                                f"-- Statistics for {col_name}\nSELECT MIN({col_name}), MAX({col_name}), AVG({col_name}) FROM {table_ref};"
                            )
                        elif data_type in ["DATE", "TIMESTAMP", "TIMESTAMPTZ"]:
                            queries.append(
                                f"-- Date range for {col_name}\nSELECT MIN({col_name}), MAX({col_name}) FROM {table_ref};"
                            )

                    result = {"table_name": table_name, "sample_queries": queries}

                    return [
                        TextContent(
                            type="text", text=json.dumps(result, indent=2, default=str)
                        )
                    ]

                elif name == "export_query_results":
                    sql = arguments.get("sql")
                    format_type = arguments.get("format", "json")

                    result = await self.executor.execute_query(sql)

                    if format_type == "csv":
                        # Convert to CSV format
                        csv_lines = []
                        csv_lines.append(",".join(result["columns"]))

                        for row in result["rows"]:
                            csv_row = []
                            for value in row:
                                if value is None:
                                    csv_row.append("")
                                else:
                                    # Escape commas and quotes
                                    str_value = str(value)
                                    if "," in str_value or '"' in str_value:
                                        str_value = (
                                            '"' + str_value.replace('"', '""') + '"'
                                        )
                                    csv_row.append(str_value)
                            csv_lines.append(",".join(csv_row))

                        csv_content = "\n".join(csv_lines)

                        return [
                            TextContent(
                                type="text",
                                text=f"CSV Export ({result['row_count']} rows):\n\n{csv_content}",
                            )
                        ]
                    else:
                        return [
                            TextContent(
                                type="text",
                                text=json.dumps(result, indent=2, default=str),
                            )
                        ]

                else:
                    raise ValueError(f"Unknown tool: {name}")

            except Exception as e:
                logger.error(f"Error calling tool {name}: {e}")
                logger.error(traceback.format_exc())

                return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def run(self):
        """Run the MCP server"""

        # Validate configuration
        if not DB_CONNECTION_STRING:
            logger.error("DB_CONNECTION_STRING environment variable is required")
            sys.exit(1)

        logger.info("Starting Vertica MCP Server...")

        # Setup handlers
        await self.setup_handlers()

        # Run server using stdio transport
        async with stdio_server() as streams:
            initialization_options = InitializationOptions(
                server_name="vertica-database",
                server_version="0.1.0",
                capabilities=self.server.get_capabilities(NotificationOptions(), {}),
            )
            await self.server.run(*streams, initialization_options)


async def async_main():
    """Async main entry point"""
    server = VerticaMCPServer()

    try:
        await server.run()
    except KeyboardInterrupt:
        logger.info("Server interrupted by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        logger.info("Vertica MCP Server shutdown complete")


def main():
    """Synchronous entry point for console scripts"""

    parser = argparse.ArgumentParser(description="Vertica Database MCP Server")
    parser.add_argument("--version", action="version", version="0.1.0")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    # Run the async main function
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
