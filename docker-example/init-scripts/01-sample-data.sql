-- Sample data setup for Vertica MCP Server testing
-- This script runs during container initialization

-- Create testuser schema and user
CREATE SCHEMA IF NOT EXISTS testuser;
CREATE USER IF NOT EXISTS testuser;
GRANT ALL ON SCHEMA testuser TO testuser;

-- Create tables in the testuser schema
CREATE TABLE testuser.employees (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    hire_date DATE DEFAULT CURRENT_DATE,
    salary NUMERIC(10,2),
    department_id INTEGER
);

CREATE TABLE testuser.departments (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(100),
    budget NUMERIC(15,2)
);

-- Insert sample data
INSERT INTO testuser.departments (id, name, location, budget) VALUES 
    (1, 'Engineering', 'San Francisco', 5000000);
INSERT INTO testuser.departments (id, name, location, budget) VALUES 
    (2, 'Marketing', 'New York', 2000000);
INSERT INTO testuser.departments (id, name, location, budget) VALUES 
    (3, 'Sales', 'Chicago', 3000000);

INSERT INTO testuser.employees (id, first_name, last_name, email, salary, department_id) VALUES 
    (1, 'John', 'Doe', 'john.doe@company.com', 75000, 1);
INSERT INTO testuser.employees (id, first_name, last_name, email, salary, department_id) VALUES 
    (2, 'Jane', 'Smith', 'jane.smith@company.com', 85000, 1);
INSERT INTO testuser.employees (id, first_name, last_name, email, salary, department_id) VALUES 
    (3, 'Bob', 'Johnson', 'bob.johnson@company.com', 65000, 2);
INSERT INTO testuser.employees (id, first_name, last_name, email, salary, department_id) VALUES 
    (4, 'Alice', 'Brown', 'alice.brown@company.com', 70000, 3);

-- Create a view for testing
CREATE VIEW testuser.employee_details AS
SELECT 
    e.id,
    e.first_name || ' ' || e.last_name AS full_name,
    e.email,
    e.hire_date,
    e.salary,
    d.name AS department_name,
    d.location
FROM testuser.employees e
JOIN testuser.departments d ON e.department_id = d.id;

-- Create projections for better performance (Vertica-specific)
CREATE PROJECTION testuser.employees_proj (
    id,
    first_name,
    last_name,
    email,
    hire_date,
    salary,
    department_id
) AS SELECT * FROM testuser.employees ORDER BY id;

CREATE PROJECTION testuser.departments_proj (
    id,
    name,
    location,
    budget
) AS SELECT * FROM testuser.departments ORDER BY id;

-- Select projections to make them active
SELECT START_REFRESH();