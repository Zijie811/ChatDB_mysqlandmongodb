use book_borrow;

-- 创建 Books 表
CREATE TABLE Books (
    book_id CHAR(4) PRIMARY KEY,
    title VARCHAR(255),
    author VARCHAR(255),
    genre VARCHAR(255),
    published_year INT,
    price int
);

-- 创建 Borrowers 表
CREATE TABLE Borrowers (
    borrower_id CHAR(5) PRIMARY KEY,
    name VARCHAR(255),
    contact_info VARCHAR(255),
    age int
);

-- 创建 Borrow_Records 表
CREATE TABLE Borrow_Records (
    record_id CHAR(4) PRIMARY KEY,
    book_id CHAR(4),
    borrower_id CHAR(5),
    borrow_date DATE,
    return_date DATE,
    FOREIGN KEY (book_id) REFERENCES Books(book_id),
    FOREIGN KEY (borrower_id) REFERENCES Borrowers(borrower_id)
);

-- 插入示例数据
INSERT INTO Books VALUES ('B001', 'To Kill a Mockingbird', 'Harper Lee', 'Fiction', 1960, 12),
                         ('B002', '1984', 'George Orwell', 'Dystopian', 1949, 9),
                         ('B003', 'The Great Gatsby', 'F. Scott Fitzgerald', 'Classic', 1925, 15);

INSERT INTO Borrowers VALUES ('BR001', 'Emily Davis', 'emilyd@example.com', 34), 
                             ('BR002', 'Michael Johnson', 'michaelj@example.com', 23),
                             ('BR003', 'Sarah Lee', 'sarahl@example.com', 45);
                             
INSERT INTO Borrow_Records VALUES ('R001', 'B001', 'BR001', '2024-02-01', '2024-02-15'),
                                  ('R002', 'B002', 'BR002', '2024-02-05', '2024-02-20'),
                                  ('R003', 'B003', 'BR003', '2024-02-10', '2024-02-25');
