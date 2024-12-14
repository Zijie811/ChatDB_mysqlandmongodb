use employee_info;

-- 创建 Departments 表
CREATE TABLE Departments (
    department_id CHAR(3) PRIMARY KEY,
    department_name VARCHAR(255)
);

-- 创建 Employees 表
CREATE TABLE Employees (
    employee_id CHAR(4) PRIMARY KEY,
    name VARCHAR(255),
    position VARCHAR(255),
    salary DECIMAL(10, 2),
    age int,
    department_id CHAR(3),
    FOREIGN KEY (department_id) REFERENCES Departments(department_id)
);

-- 创建 Projects 表
CREATE TABLE Projects (
    project_id CHAR(4) PRIMARY KEY,
    project_name VARCHAR(255),
    department_id CHAR(3),
    person_involved int,
    FOREIGN KEY (department_id) REFERENCES Departments(department_id)
);

-- 创建 Employee_Project 连接表
CREATE TABLE Employee_Project (
    employee_id CHAR(4),
    project_id CHAR(4),
    PRIMARY KEY (employee_id, project_id),
    FOREIGN KEY (employee_id) REFERENCES Employees(employee_id),
    FOREIGN KEY (project_id) REFERENCES Projects(project_id)
);

-- 插入示例数据
INSERT INTO Departments VALUES ('D01', 'Management'), ('D02', 'Engineering');

INSERT INTO Employees VALUES
('E001', 'Alice Johnson', 'Manager', 70000, 31, 'D01'),
('E002', 'Bob Williams', 'Engineer', 60000, 27, 'D02'),
('E003', 'Charlie Brown', 'Technician', 50000, 25, 'D02');

INSERT INTO Projects VALUES ('P001', 'Product Development', 'D02', 3), ('P002', 'Market Research', 'D01', 2);

INSERT INTO Employee_Project VALUES ('E001', 'P002'), ('E002', 'P001'), ('E003', 'P001');
