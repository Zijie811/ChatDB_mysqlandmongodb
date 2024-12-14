use coffee_sales;
-- 创建coffee sales表
CREATE TABLE Customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    contact_info VARCHAR(255)
);

-- 创建 Products 表psasq
CREATE TABLE Products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(255),
    category VARCHAR(255),
    price DECIMAL(10, 2)
);

-- 创建 Sales 表，关联 Customers 和 Products
CREATE TABLE Sales (
    sale_id INT PRIMARY KEY AUTO_INCREMENT,
    sale_date DATE,
    amount DECIMAL(10, 2),
    product_id INT,
    customer_id INT,
    FOREIGN KEY (product_id) REFERENCES Products(product_id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

-- 插入示例数据
INSERT INTO Customers (customer_id, customer_name, contact_info) VALUES
(1, 'John Doe', 'johndoe@example.com'),
(2, 'Jane Smith', 'janesmith@example.com');

INSERT INTO Products (product_id, product_name, category, price) VALUES
(1, 'Espresso', 'Drink', 2.5),
(2, 'Cappuccino', 'Drink', 3.5),
(3, 'Blueberry Muffin', 'Food', 2.0);

INSERT INTO Sales (sale_id, sale_date, amount, product_id, customer_id) VALUES
(1, '2024-01-01', 10.5, 1, 1),
(2, '2024-01-02', 15.0, 2, 2),
(3, '2024-01-03', 8.75, 3, 1);

