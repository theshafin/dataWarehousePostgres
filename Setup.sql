CREATE TYPE GENDER AS ENUM ('male', 'female', 'other', 'ND');

CREATE TABLE customers (
    customer_id INT UNIQUE PRIMARY KEY,
    first_name TEXT,
    last_name TEXT NOT NULL,
    gender GENDER NOT NULL,
)

CREATE TABLE stocks (
    stock_code TEXT UNIQUE PRIMARY KEY,
    prd_desc TEXT,
    inventory INT NOT NULL,
    base_unit_price DECIMAL NOT NULL
)

CREATE TABLE invoices (
    invoice_id INT PRIMARY KEY,
    stock_code TEXT REFERENCES stocks(stock_code),
    quantity INT NOT NULL,
    invoice_date TIMESTAMP NOT NULL,
    unit_price DECIMAL NOT NULL,
    customer_id INT REFERENCES customers(customer_id),
    country TEXT
)

CREATE TABLE template_1 (
    InvoiceNo TEXT,
    StockCode TEXT,
    quantity INT NOT NULL,
    invoice_date TIMESTAMP NOT NULL,
    unit_price DECIMAL NOT NULL,
    customer_id INT,
    country TEXT
)