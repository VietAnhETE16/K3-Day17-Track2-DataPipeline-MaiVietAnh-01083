VENV    := .venv

ifeq ($(OS),Windows_NT)
    VENV_BIN := $(VENV)/Scripts
    PY_HOST  := python
else
    SHELL    := /bin/bash
    VENV_BIN := $(VENV)/bin
    PY_HOST  := python3
endif

PY      := $(VENV_BIN)/python
PIP     := $(VENV_BIN)/pip
DBT     := $(VENV_BIN)/dbt

export PYTHONUTF8 := 1
export LAB17_DB := $(CURDIR)/warehouse.duckdb
export DBT_PROFILES_DIR := $(CURDIR)/dbt

.DEFAULT_GOAL := help
.PHONY: help setup seed seed-extra pipeline verify quick explain plan dbt-test \
        dbt-docs crash-test compact reset clean

help:  ## danh sách lệnh
ifeq ($(OS),Windows_NT)
	@echo.
	@echo   LAB 17 — Data Pipeline Engineering
	@echo.
	@$(PY_HOST) -c "import re; [print(f'    \033[36m{m[1]:<14}\033[0m {m[2]}') for line in open('Makefile', encoding='utf-8') if (m := re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line))]"
	@echo.
else
	@echo ""
	@echo "  LAB 17 — Data Pipeline Engineering"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[36m%-14s\033[0m %s\n", $$1, $$2}'
	@echo ""
endif

setup:  ## venv + thư viện + sinh dữ liệu (chạy một lần)
ifeq ($(OS),Windows_NT)
	@$(PY_HOST) -c "import os, sys, subprocess; subprocess.run([sys.executable, '-m', 'venv', '$(VENV)']) if not os.path.exists('$(VENV)') else None"
	@$(PY) -m pip install -q --upgrade pip
	@$(PY) -m pip install -q -r requirements.txt
	@$(PY) seed/generate.py
	@echo.
	@echo   xong. Bước tiếp theo:  make pipeline  rồi  make verify
else
	@test -d $(VENV) || python3 -m venv $(VENV)
	@$(PIP) install -q --upgrade pip
	@$(PIP) install -q -r requirements.txt
	@$(PY) seed/generate.py
	@echo ""
	@echo "  xong. Bước tiếp theo:  make pipeline  rồi  make verify"
endif

seed:  ## sinh lại dữ liệu seed
	@$(PY) seed/generate.py

seed-extra:  ## sinh thêm dữ liệu cho bài mở rộng trong EXTRA.md (~30 giây)
	@$(PY) seed/generate.py --extra
	@$(PY) tools/explain.py --save-baseline

pipeline:  ## chạy đường ống một lượt (14 ngày vận hành)
	@$(PY) tools/run_pipeline.py

verify:  ## ⭐ xoá kho, chạy 3 lượt, in bảng chấm — dùng lệnh này liên tục
	@$(PY) tools/verify.py

quick:  ## như verify nhưng chỉ 1 lượt (nhanh, không kiểm tra tính ổn định)
	@$(PY) tools/verify.py --runs 1

explain:  ## [mở rộng] đo rows scanned của queries/dashboard.sql
	@$(PY) tools/explain.py

plan:  ## [mở rộng] explain + in cây EXPLAIN ANALYZE
	@$(PY) tools/explain.py --plan

compact:  ## [mở rộng] chạy tools/compact.py
	@$(PY) tools/compact.py

dbt-test:  ## chạy dbt test
	@$(DBT) test --project-dir dbt --profiles-dir dbt --target-path dbt/target --log-path dbt/logs

dbt-docs:  ## dựng và mở tài liệu dbt (tuỳ chọn)
	@$(DBT) docs generate --project-dir dbt --profiles-dir dbt --target-path dbt/target --log-path dbt/logs \
	  && $(DBT) docs serve --project-dir dbt --profiles-dir dbt --target-path dbt/target

crash-test:  ## [mở rộng] kịch bản consumer bị giết giữa batch
	@$(PY) tools/crash_test.py

reset:  ## xoá kho DuckDB (giữ nguyên seed và data/)
ifeq ($(OS),Windows_NT)
	@$(PY_HOST) -c "import pathlib; [pathlib.Path(f).unlink(missing_ok=True) for f in ['warehouse.duckdb', 'warehouse.duckdb.wal']]"
	@echo   kho đã xoá.
else
	@rm -f warehouse.duckdb warehouse.duckdb.wal
	@echo "  kho đã xoá."
endif

clean:  ## xoá kho + target dbt + thư mục làm việc của crash-test
ifeq ($(OS),Windows_NT)
	@$(PY_HOST) -c "import shutil, pathlib; [shutil.rmtree(f, ignore_errors=True) if pathlib.Path(f).is_dir() else pathlib.Path(f).unlink(missing_ok=True) for f in ['warehouse.duckdb', 'warehouse.duckdb.wal', 'dbt/target', 'dbt/logs', 'data/crash']]"
	@echo   đã dọn.
else
	@rm -rf warehouse.duckdb warehouse.duckdb.wal dbt/target dbt/logs data/crash
	@echo "  đã dọn."
endif
