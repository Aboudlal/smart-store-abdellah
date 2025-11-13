# 📊 Pro Analytics 02 -- Python Starter Repository

**Professional Python project setup and Business Intelligence
workflow.**

------------------------------------------------------------------------

## 🔗 Resources

-   **Starter Repo**
-   **Project Structure**

### 🧠 Skills we practice:

Environment Management · Code Quality · Documentation · Testing ·
Version Control

------------------------------------------------------------------------

## ⚙️ Workflow 1 -- Set Up Your Machine

Follow the setup guide to prepare your tools:

👉 **SET UP MACHINE**

------------------------------------------------------------------------

## 📂 Workflow 2 -- Set Up Your Project

Initialize your project and environment:

``` bash
uv venv
uv python pin 3.12
uv sync --extra dev --extra docs --upgrade
uv run pre-commit install
uv run python --version
```

**Activate environment:** - Windows: `.\.venv\Scripts\activate` -
macOS/Linux: `source .venv/bin/activate`

------------------------------------------------------------------------

## 🔄 Workflow 3 -- Daily Workflow

### 3.1 Sync & Pull

``` bash
git pull
uv sync --extra dev --extra docs --upgrade
```

### 3.2 Run Checks

``` bash
git add .
uvx ruff check --fix
uv run pre-commit run --all-files
uv run pytest
```

### 3.3 Documentation

``` bash
uv run mkdocs build --strict
uv run mkdocs serve
```

### 3.4 Run Demo Modules

``` bash
uv run python -m src.analytics_project.demo_module_basics
uv run python -m src.analytics_project.demo_module_stats
```

### 3.5 Run Data Preparation Modules

Execute the cleaning pipeline for each dataset individually:

``` bash
uv run python -m src.analytics_project.data_preparation.prepare_customers_data
uv run python -m src.analytics_project.data_preparation.prepare_products_data
uv run python -m src.analytics_project.data_preparation.prepare_sales_data
```

------------------------------------------------------------------------

## 🚀 Workflow 4 -- Module 3: Data Cleaning and ETL Preparation

### 🎯 Goal:

Read raw CSV files into pandas DataFrames, clean the data using reusable
logic, and save the prepared version for the next step of the ETL
process.

### ✅ Steps Completed

-   Created cleaning orchestrator: `src/analytics_project/data_prep.py`
-   Implemented customer, product, and sales cleaning
-   Configured logging (`project.log`)
-   Added unit tests in `tests/test_data_scrubber.py`
-   Ran the cleaning pipeline:

``` bash
uv run python -m src.analytics_project.data_prep
```

------------------------------------------------------------------------

## 🎯 Results

Prepared files saved in `data/prepared/`:

-   `customers_prepared.csv`
-   `products_prepared.csv`
-   `sales_prepared.csv`

------------------------------------------------------------------------

# ⭐ Workflow 5 -- Module 4: Data Warehouse (P4)

## 🎯 Objective

Design and implement a full **Data Warehouse (DW)** using a **star
schema** and load cleaned datasets into SQLite to support BI analytics.

------------------------------------------------------------------------

## 1️⃣ Design Choices

### ⭐ Star Schema

Chosen because it provides:

-   Fast analytical performance\
-   Clear relationship modeling\
-   Easy BI integration

### ⭐ Full Column Version

The DW keeps **all columns** from the cleaned CSVs to preserve business
richness.

------------------------------------------------------------------------

## 2️⃣ Schema Implementation

DW file located at:

    data/dw/smart_store_dw.db

### 📐 Final Schema (Full Version)

#### Customer Dimension

``` sql
CREATE TABLE customer (
    customer_id INTEGER PRIMARY KEY,
    name TEXT,
    region TEXT,
    join_date TEXT,
    loyalty_points INTEGER,
    preferred_contact_method TEXT
);
```

#### Product Dimension

``` sql
CREATE TABLE product (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price REAL,
    stock_quantity INTEGER,
    supplier_name TEXT
);
```

#### Sales Fact Table

``` sql
CREATE TABLE sale (
    sale_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    store_id INTEGER,
    campaign_id INTEGER,
    sale_date TEXT,
    sale_amount REAL,
    discount_percent REAL,
    payment_type TEXT,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);
```

------------------------------------------------------------------------

## 3️⃣ ETL Pipeline

ETL file:

    src/analytics_project/etl_to_dw.py

### ✔ Tasks performed:

-   Create DW folder & DB\
-   Create tables\
-   Drop duplicates using `drop_duplicates()`\
-   Load CSVs\
-   Insert into DW

### ▶️ Run ETL:

``` bash
uv run python -m src.analytics_project.etl_to_dw
```

------------------------------------------------------------------------

## 4️⃣ Validation (Task 3)

Using SQLite Viewer in VS Code, I ran:

``` sql
SELECT name FROM sqlite_master WHERE type='table';
PRAGMA table_info(customer);
PRAGMA table_info(product);
PRAGMA table_info(sale);
SELECT COUNT(*) FROM customer;
SELECT COUNT(*) FROM product;
SELECT COUNT(*) FROM sale;
SELECT * FROM sale LIMIT 10;
```

### 📸 Screenshots

Screenshots are stored in `docs/images/`:

-   `dw_customer_table.png`
-   `dw_product_table.png`
-   `dw_sale_table.png`

To display in README:

``` markdown
![Customer](docs/images/dw_customer_table.png)
![Product](docs/images/dw_product_table.png)
![Sale](docs/images/dw_sale_table.png)
```

------------------------------------------------------------------------

## ⚠️ Challenges & Solutions

### 🔴 Challenge: Duplicate Primary Keys

`UNIQUE constraint failed` appeared for customer_id, product_id, and
sale_id.

### 🟢 Solution:

Used pandas:

``` python
df.drop_duplicates(subset=["CustomerID"])
```

This ensured clean inserts.

------------------------------------------------------------------------

## 📤 Git Workflow

``` bash
git add .
git commit -m "feat: add DW design, ETL pipeline, validation screenshots"
git push
```

------------------------------------------------------------------------

## 👨‍💻 Author

**Abdellah Boudlal**\
GitHub: https://github.com/Aboudlal
