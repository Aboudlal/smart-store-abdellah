# 📊 Pro Analytics 02 – Python Starter Repository

**Professional Python project setup and Business Intelligence workflow.**

---

## 🔗 Resources
- **Starter Repo**
- **Project Structure**

### 🧠 Skills we practice:
Environment Management · Code Quality · Documentation · Testing · Version Control

---

## ⚙️ Workflow 1 – Set Up Your Machine

Follow the setup guide to prepare your tools:

👉 **SET UP MACHINE**

---

## 📂 Workflow 2 – Set Up Your Project

Initialize your project and environment:

```bash
uv venv
uv python pin 3.12
uv sync --extra dev --extra docs --upgrade
uv run pre-commit install
uv run python --version
```

**Activate environment:**
- Windows: `.\.venv\Scripts\activate`
- macOS/Linux: `source .venv/bin/activate`

---

## 🔄 Workflow 3 – Daily Workflow

### 3.1 Sync & Pull
```bash
git pull
uv sync --extra dev --extra docs --upgrade
```

### 3.2 Run Checks
```bash
git add .
uvx ruff check --fix
uv run pre-commit run --all-files
uv run pytest
```

### 3.3 Documentation
```bash
uv run mkdocs build --strict
uv run mkdocs serve
```

### 3.4 Run Demo Modules
```bash
uv run python -m src.analytics_project.demo_module_basics
uv run python -m src.analytics_project.demo_module_stats
```

### 3.5 Run Data Preparation Modules
Execute the cleaning pipeline for each dataset individually:

```bash
# Prepare Customer Data
uv run python -m src.analytics_project.data_preparation.prepare_customers_data

# Prepare Product Data
uv run python -m src.analytics_project.data_preparation.prepare_products_data

# Prepare Sales Data
uv run python -m src.analytics_project.data_preparation.prepare_sales_data
```

---

## 🚀 Workflow 4 – Module 3: Data Cleaning and ETL Preparation

### 🎯 Goal:
Read raw CSV files into pandas DataFrames, clean the data using reusable logic, and save the prepared version for the next step of the ETL process.

### ✅ Steps Completed
- Created core cleaning module: `src/analytics_project/data_prep.py`  
  Acts as the orchestrator that executes the cleaning logic for customers, products, and sales datasets using the **DataScrubber** class.
  - `read_csv_to_df()` → read one CSV safely
  - `load_raw_dataframes()` → read all CSVs in `data/raw/`
  - `clean_customer_data()` → specific cleaning for customers
  - `clean_product_data()` → specific cleaning for products
  - `clean_sales_data()` → specific cleaning for sales
- Configured logging to `project.log`
- Implemented and passed Unit Tests (`tests/test_data_scrubber.py`)
- Executed the module:  
  ```bash
  uv run python -m src.analytics_project.data_prep
  ```
- Git workflow:
  ```bash
  git add .
  git commit -m "feat: implement module 3 data cleaning pipeline and unit tests"
  git push
  ```

---

## 🔹 How to Run Data Preparation (Task 4)
Run the data pipeline directly from the root project folder:
```bash
uv run python -m src.analytics_project.data_prep
```

---

## 🔹 How to Run Unit Tests
Confirm the robustness of the cleaning logic by running all unit tests:
```bash
uv run python -m unittest discover tests
```

---

## 🎯 Results
- **Pipeline Success:** All raw data files are successfully read, cleaned, and saved.
- **Fichiers de sortie préparés:**  
  Les fichiers nettoyés sont stockés dans le dossier `data/prepared/` et sont prêts pour l'analyse:
  - `customers_prepared.csv`
  - `products_prepared.csv`
  - `sales_prepared.csv`

  *(Path d'exemple : `C:\Repos\smart-store-abdellah\data\prepared\customers_prepared.csv`)*

- **Test Coverage:** All unit tests for the core cleaning logic (DataScrubber) pass successfully.

---

## 👨‍💻 Author
**Abdellah Boudlal**  
🌐 [GitHub Profile](https://github.com/Aboudlal)
