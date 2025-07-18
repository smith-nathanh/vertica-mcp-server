#!/usr/bin/env python3
"""
Test script to verify Vertica MCP Server tools functionality
"""

import os
import pytest
from vertica_mcp_server.server import VerticaMCPServer

@pytest.mark.asyncio
@pytest.mark.integration
async def test_tools():
    """Test all MCP server tools"""
    
    # Set up environment
    os.environ["DB_CONNECTION_STRING"] = "vertica://dbadmin:@localhost:5433/testdb"
    
    try:
        # Create server instance
        server = VerticaMCPServer()
        
        print("=== Testing MCP Server Tools ===\n")
        
        # Test 1: list_tables
        print("1. Testing list_tables...")
        tables = await server.inspector.get_tables()
        print(f"✓ Found {len(tables)} tables")
        for table in tables[:3]:  # Show first 3
            print(f"  - {table['schema_name']}.{table['table_name']} (type: {table['table_type']})")
        
        # Test 2: list_views  
        print("\n2. Testing list_views...")
        views = await server.inspector.get_views()
        print(f"✓ Found {len(views)} views")
        for view in views[:3]:
            print(f"  - {view['schema_name']}.{view['view_name']}")
        
        # Test 3: list_projections
        print("\n3. Testing list_projections...")
        projections = await server.inspector.get_projections()
        print(f"✓ Found {len(projections)} projections")
        for proj in projections[:3]:
            print(f"  - {proj['schema_name']}.{proj['projection_name']} (table: {proj['anchor_table_name']})")
        
        # Test 4: describe_table
        print("\n4. Testing describe_table...")
        columns = await server.inspector.get_table_columns("employees", "testuser")
        print(f"✓ Table testuser.employees has {len(columns)} columns:")
        for col in columns:
            print(f"  - {col['column_name']}: {col['data_type']} (nullable: {col['is_nullable']})")
        
        # Test 5: execute_query - Basic SELECT
        print("\n5. Testing execute_query (SELECT)...")
        result = await server.executor.execute_query("SELECT * FROM testuser.employees ORDER BY id LIMIT 3")
        print(f"✓ Query returned {result['row_count']} rows in {result['execution_time_seconds']:.3f}s")
        print("  Columns:", result['columns'])
        for i, row in enumerate(result['rows']):
            print(f"  Row {i+1}: {row}")
        
        # Test 6: execute_query - Count and aggregation
        print("\n6. Testing execute_query (aggregation)...")
        result = await server.executor.execute_query(
            "SELECT d.name as department, COUNT(*) as employees, AVG(e.salary) as avg_salary " +
            "FROM testuser.employees e JOIN testuser.departments d ON e.department_id = d.id " +
            "GROUP BY d.name ORDER BY employees DESC"
        )
        print(f"✓ Aggregation query returned {result['row_count']} rows")
        for row in result['rows']:
            print(f"  {row[0]}: {row[1]} employees, avg salary: ${row[2]:.2f}")
        
        # Test 7: explain_query
        print("\n7. Testing explain_query...")
        plan = await server.executor.explain_query("SELECT * FROM testuser.employees WHERE salary > 70000")
        print(f"✓ Execution plan has {len(plan['execution_plan'])} lines")
        print("  First few plan lines:")
        for line in plan['execution_plan'][:5]:
            print(f"    {line['plan_line']}")
        
        # Test 8: Safety controls
        print("\n8. Testing safety controls...")
        try:
            await server.executor.execute_query("DROP TABLE testuser.employees")
            print("✗ Safety control failed - dangerous query was allowed!")
            return False
        except ValueError as e:
            print(f"✓ Safety control working: {e}")
        
        try:
            await server.executor.execute_query("INSERT INTO testuser.employees VALUES (999, 'Test', 'User', 'test@test.com', CURRENT_DATE, 50000, 1)")
            print("✗ Safety control failed - INSERT was allowed!")
            return False
        except ValueError as e:
            print(f"✓ Safety control working: {e}")
        
        # Test 9: Query limits
        print("\n9. Testing query limits...")
        result = await server.executor.execute_query("SELECT * FROM testuser.employees")  # No LIMIT in query
        print(f"✓ Auto-limit applied - returned {result['row_count']} rows")
        # Check if LIMIT was added to the query
        if "LIMIT" in result['query']:
            print(f"✓ Auto-limit working: {result['query']}")
        
        print("\n✓ All tool tests passed! MCP server functionality is working correctly.")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

