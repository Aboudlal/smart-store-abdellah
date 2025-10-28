# Pro Analytics 02 Python Starter Repository

> Use this repo to start a professional Python project.

- Additional information: <https://github.com/denisecase/pro-analytics-02>
- Project organization: [STRUCTURE](./STRUCTURE.md)
- Build professional skills:
  - **Environment Management**: Every project in isolation
  - **Code Quality**: Automated checks for fewer bugs
  - **Documentation**: Use modern project documentation tools
  - **Testing**: Prove your code works
  - **Version Control**: Collaborate professionally

---

## WORKFLOW 1. Set Up Your Machine

Proper setup is critical.
Complete each step in the following guide and verify carefully.

- [SET UP MACHINE](./SET_UP_MACHINE.md)

---

## WORKFLOW 2. Set Up Your Project

After verifying your machine is set up, set up a new Python project by copying this template.
Complete each step in the following guide.

- [SET UP PROJECT](./SET_UP_PROJECT.md)

It includes the critical commands to set up your local environment (and activate it):

```shell
uv venv
uv python pin 3.12
uv sync --extra dev --extra docs --upgrade
uv run pre-commit install
uv run python --version
```

**Windows (PowerShell):**
```shell
.\.venv\Scripts\activate
```

**macOS / Linux / WSL:**
```shell
source .venv/bin/activate
```

---

## WORKFLOW 3. Daily Workflow

Please ensure that the prior steps have been verified before continuing.
When working on a project, we open just that project in VS Code.

### 3.1 Git Pull from GitHub

Always start with `git pull` to check for any changes made to the GitHub repo.

```shell
git pull
```

### 3.2 Run Checks as You Work

This mirrors real work where we typically:

1. Update dependencies (for security and compatibility).
2. Clean unused cached packages to free space.
3. Use `git add .` to stage all changes.
4. Run ruff and fix minor issues.
5. Update pre-commit periodically.
6. Run pre-commit quality checks on all code files (**twice if needed**, the first pass may fix things).
7. Run tests.

```shell
uv sync --extra dev --extra docs --upgrade
uv cache clean
git add .
uvx ruff check --fix
uvx pre-commit autoupdate
uv run pre-commit run --all-files
git add .
uv run pytest
```

---

### 3.3 Build Project Documentation

```shell
uv run mkdocs build --strict
uv run mkdocs serve
```

- Open the local docs URL in your browser to view.  
- Stop hosting with `CTRL c`.  

---

### 3.4 Execute Demo Modules

```shell
uv run python -m analytics_project.demo_module_basics
uv run python -m analytics_project.demo_module_languages
uv run python -m analytics_project.demo_module_stats
uv run python -m analytics_project.demo_module_viz
```

---

### 3.5 Git add-commit-push to GitHub

```shell
git add .
git commit -m "describe your change in quotes"
git push -u origin main
```

---

### 3.6 Modify and Debug

Pull updates, keep tools updated, and add-commit-push frequently.

---

## 🚀 WORKFLOW 4. Module P2 – Reading Raw Data

Today I implemented **P2 – Data Ingestion**.  
The goal is to read raw CSV files into pandas DataFrames with reusable functions and logging.

### Steps Completed

1. **Created new module**  
   - `src/analytics_project/data_prep.py`  
   - Added functions:  
     - `read_csv_to_df()` → read one CSV safely  
     - `load_raw_dataframes()` → read all CSVs in `data/raw/`  
   - Configured logging to `project.log`

2. **Executed the module**  
   ```shell
   uv run python -m analytics_project.data_prep
   ```  
   - Verified shapes of DataFrames in logs

3. **Git Workflow**  
   ```shell
   git add .
   git commit -m "add starter files"
   git push
   ```

4. **Updated README.md** with new commands and workflow.

---


### 🔹 How to Execute the Module (Task 4)

Use the terminal to run the initial `data_prep.py` module directly.  
Always open the terminal **from the root project folder**.

```bash
uv run python -m analytics_project.data_prep
```

After running:
- Check the **terminal output** and the **project.log** file.  
- Verify that a DataFrame was created for each raw data file.  
- Confirm the shape of each DataFrame is logged correctly.


### ✅ Results
- All CSV files in `data/raw/` now load automatically into pandas DataFrames.  
- Logs record file names, shapes, and column lists.  
- `project.log` created at the repo root (ignored in `.gitignore`).  

---

## 👨‍💻 Author
- **Abdellah Boudlal**  
- [GitHub Profile](https://github.com/Aboudlal)
