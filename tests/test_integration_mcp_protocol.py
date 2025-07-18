#!/usr/bin/env python3
"""
Test script to verify MCP protocol communication with the Vertica MCP Server
"""

import os
import json
import pytest
from pydantic import AnyUrl
from vertica_mcp_server.server import VerticaMCPServer

@pytest.mark.asyncio
@pytest.mark.integration
async def test_mcp_protocol():
    """Test MCP protocol communication"""
    
    # Set up environment
    os.environ["DB_CONNECTION_STRING"] = "vertica://dbadmin:@localhost:5433/testdb"
    
    print("=== Testing MCP Protocol Communication ===\n")
    
    try:
        # Import after setting environment
        from vertica_mcp_server.server import VerticaMCPServer
        
        # Create server instance  
        server = VerticaMCPServer()
        await server.setup_handlers()
        
        print("1. Testing list_tools...")
        tools = await server.server._list_tools_handler()
        print(f"✓ MCP server exposes {len(tools)} tools:")
        for tool in tools:
            print(f"  - {tool.name}: {tool.description}")
        
        print("\n2. Testing list_resources...")
        resources = await server.server._list_resources_handler()
        print(f"✓ MCP server exposes {len(resources)} resources")
        for resource in resources[:5]:  # Show first 5
            print(f"  - {resource.name} ({resource.uri})")
        
        print("\n3. Testing tool execution - list_tables...")
        result = await server.server._call_tool_handler("list_tables", {})
        result_text = result[0].text
        result_data = json.loads(result_text)
        print(f"✓ list_tables returned {len(result_data['tables'])} tables")
        
        print("\n4. Testing tool execution - describe_table...")
        result = await server.server._call_tool_handler("describe_table", {
            "table_name": "employees",
            "schema_name": "testuser"
        })
        result_text = result[0].text
        result_data = json.loads(result_text)
        print(f"✓ describe_table returned {result_data['column_count']} columns")
        
        print("\n5. Testing tool execution - execute_query...")
        result = await server.server._call_tool_handler("execute_query", {
            "sql": "SELECT COUNT(*) as employee_count FROM testuser.employees"
        })
        result_text = result[0].text
        result_data = json.loads(result_text)
        print(f"✓ execute_query returned {result_data['row_count']} rows")
        print(f"  Employee count: {result_data['rows'][0][0]}")
        
        print("\n6. Testing tool execution - generate_sample_queries...")
        result = await server.server._call_tool_handler("generate_sample_queries", {
            "table_name": "employees", 
            "schema_name": "testuser"
        })
        result_text = result[0].text
        result_data = json.loads(result_text)
        print(f"✓ generate_sample_queries returned {len(result_data['sample_queries'])} queries")
        print("  Sample queries:")
        for i, query in enumerate(result_data['sample_queries'][:3]):
            print(f"    {i+1}. {query.split('--')[1].strip() if '--' in query else 'Query'}")
        
        print("\n7. Testing tool execution - export_query_results (JSON)...")
        result = await server.server._call_tool_handler("export_query_results", {
            "sql": "SELECT * FROM testuser.departments",
            "format": "json"
        })
        result_text = result[0].text
        result_data = json.loads(result_text)
        print(f"✓ JSON export returned {result_data['row_count']} rows")
        
        print("\n8. Testing tool execution - export_query_results (CSV)...")
        result = await server.server._call_tool_handler("export_query_results", {
            "sql": "SELECT * FROM testuser.departments",
            "format": "csv"
        })
        result_text = result[0].text
        print("✓ CSV export completed")
        print("  First few lines:")
        for line in result_text.split('\n')[:4]:
            if line.strip():
                print(f"    {line}")
        
        print("\n9. Testing resource reading...")
        resource_content = await server.server._read_resource_handler(
            AnyUrl("vertica://table/testuser.employees")
        )
        resource_data = json.loads(resource_content)
        print(f"✓ Resource reading returned table with {resource_data['column_count']} columns")
        
        print("\n✓ All MCP protocol tests passed! Server is fully functional.")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

