# 📊 Pro Analytics 02 – Python Starter Repository

> Professional Python project setup and Business Intelligence workflow.

🔗 Resources:  
- [Starter Repo](https://github.com/denisecase/pro-analytics-02)  
- [Project Structure](./STRUCTURE.md)  

Skills we practice: **Environment Management · Code Quality · Documentation · Testing · Version Control**

---

## ⚙️ Workflow 1 – Set Up Your Machine
Follow the setup guide to prepare your tools:  
👉 [SET UP MACHINE](./SET_UP_MACHINE.md)

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
uv run python -m analytics_project.demo_module_basics
uv run python -m analytics_project.demo_module_stats
```

## 🚀 Workflow 4 – Module P2: Reading Raw Data

**Goal:** Read raw CSV files into pandas DataFrames with reusable functions and logging.  

### ✅ Steps Completed
1. Created new module: `src/analytics_project/data_prep.py`  
   - `read_csv_to_df()` → read one CSV safely  
   - `load_raw_dataframes()` → read all CSVs in `data/raw/`  
   - Configured logging to `project.log`  

2. Executed the module:
```bash
uv run python -m analytics_project.data_prep
```

3. Git workflow:
```bash
git add .
git commit -m "add starter files"
git push
```

---

### 🔹 How to Execute the Module (Task 4)

Run directly from the **root project folder**:

```bash
uv run python -m analytics_project.data_prep
```

Then check:
- Terminal output  
- `project.log` file  
- Confirm each CSV was loaded and shapes logged  

---

### 🎯 Results
- All CSV files in `data/raw/` load into pandas DataFrames  
- Shapes and columns recorded in logs  
- `project.log` created at repo root (ignored in `.gitignore`)  

---

## 👨‍💻 Author
**Abdellah Boudlal**  
🌐 [GitHub Profile](https://github.com/Aboudlal)  
