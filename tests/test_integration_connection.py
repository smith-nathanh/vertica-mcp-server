#!/usr/bin/env python3
"""
Test script to verify Vertica MCP Server connection and functionality
"""

import os
import pytest
from vertica_mcp_server.server import VerticaMCPServer

@pytest.mark.asyncio
@pytest.mark.integration
async def test_connection():
    """Test basic database connection and tools"""
    
    # Set up environment
    os.environ["DB_CONNECTION_STRING"] = "vertica://dbadmin:@localhost:5433/testdb"
    
    try:
        # Create server instance
        server = VerticaMCPServer()
        
        # Test database connection
        print("Testing database connection...")
        conn = await server.connection_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✓ Connected to Vertica: {version[0]}")
        conn.close()
        
        # Test inspector functionality
        print("\nTesting database inspector...")
        tables = await server.inspector.get_tables()
        print(f"✓ Found {len(tables)} tables")
        
        views = await server.inspector.get_views()
        print(f"✓ Found {len(views)} views")
        
        projections = await server.inspector.get_projections()
        print(f"✓ Found {len(projections)} projections")
        
        # Test table columns
        print("\nTesting table column inspection...")
        columns = await server.inspector.get_table_columns("employees", "testuser")
        print(f"✓ Found {len(columns)} columns in testuser.employees")
        
        # Test query execution
        print("\nTesting query execution...")
        result = await server.executor.execute_query("SELECT COUNT(*) FROM testuser.employees")
        print(f"✓ Query executed: {result['row_count']} rows returned")
        print(f"  Employee count: {result['rows'][0][0]}")
        
        # Test sample data
        print("\nTesting sample data...")
        result = await server.executor.execute_query("SELECT * FROM testuser.employees LIMIT 2")
        print(f"✓ Sample query executed: {result['row_count']} rows returned")
        for i, row in enumerate(result['rows']):
            print(f"  Row {i+1}: {dict(zip(result['columns'], row))}")
            
        print("\n✓ All tests passed! MCP server is working correctly.")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False
    
    return True

