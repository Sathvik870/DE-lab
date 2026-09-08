#!/usr/bin/env bash
# ==============================================================================
# Local CI Runner: Automated Testing for Data Pipelines
# ==============================================================================
# Class Comments:
# A Local CI Runner enables data engineers to execute the exact same test gates
# locally that run in GitHub Actions. This fast-feedback loop catches errors
# before git commits and prevents dirty pipeline code from reaching CI/CD.
# ==============================================================================

set -eo pipefail

# ANSI color codes for rich terminal feedback
BOLD="\033[1m"
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[0;33m"
BLUE="\033[0;34m"
CYAN="\033[0;36m"
NC="\033[0m" # No Color

echo -e "${BOLD}${BLUE}==============================================================================${NC}"
echo -e "${BOLD}${BLUE}      EARBUDS DATA PIPELINE: LOCAL CI/CD AUTOMATED TEST RUNNER                ${NC}"
echo -e "${BOLD}${BLUE}==============================================================================${NC}"

# Detect Python interpreter
if [ -f ".venv/bin/python" ]; then
    PYTHON=".venv/bin/python"
    PYTEST=".venv/bin/pytest"
    FLAKE8=".venv/bin/flake8"
elif [ -f "venv/bin/python" ]; then
    PYTHON="venv/bin/python"
    PYTEST="venv/bin/pytest"
    FLAKE8="venv/bin/flake8"
else
    PYTHON="python3"
    PYTEST="pytest"
    FLAKE8="flake8"
fi

echo -e "${CYAN}[INFO] Using Python interpreter:${NC} $PYTHON"
echo -e "${CYAN}[INFO] Using Pytest executable   :${NC} $PYTEST"

FAILED_STAGES=()

# ------------------------------------------------------------------------------
# STAGE 1: Code Linting & Static Analysis
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${CYAN}▶ [STAGE 1/4] Running Code Linting & Style Checks (Flake8)...${NC}"
if command -v "$FLAKE8" &> /dev/null || [ -f "$FLAKE8" ]; then
    if $FLAKE8 etl/ tests/ --max-line-length=120 --exclude=__pycache__,.venv,venv; then
        echo -e "${GREEN}✔ [STAGE 1] Linting passed with 0 errors.${NC}"
    else
        echo -e "${RED}✘ [STAGE 1] Linting failed. Please fix style/syntax errors above.${NC}"
        FAILED_STAGES+=("Stage 1: Linting")
    fi
else
    echo -e "${YELLOW}⚠ [STAGE 1] flake8 not found in environment. Skipping static analysis.${NC}"
fi

# ------------------------------------------------------------------------------
# STAGE 2: Unit Testing (Transformation & Validation Logic)
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${CYAN}▶ [STAGE 2/4] Executing Unit Tests (Transformations & Validations)...${NC}"
if $PYTEST tests/test_transform.py tests/test_validator.py -v; then
    echo -e "${GREEN}✔ [STAGE 2] All transformation & validation unit tests passed.${NC}"
else
    echo -e "${RED}✘ [STAGE 2] Unit tests failed.${NC}"
    FAILED_STAGES+=("Stage 2: Unit Tests")
fi

# ------------------------------------------------------------------------------
# STAGE 3: Mock Testing (Database Extract, Load & Airflow DAG Integrity)
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${CYAN}▶ [STAGE 3/4] Executing Mock Tests & DAG Integrity...${NC}"
if $PYTEST tests/test_extract_mock.py tests/test_loader_mock.py tests/test_pipeline_mock.py tests/test_dag_integrity.py -v; then
    echo -e "${GREEN}✔ [STAGE 3] All mock and DAG integrity tests passed.${NC}"
else
    echo -e "${RED}✘ [STAGE 3] Mock tests failed.${NC}"
    FAILED_STAGES+=("Stage 3: Mock Tests")
fi

# ------------------------------------------------------------------------------
# STAGE 4: Test Coverage Quality Gate
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${CYAN}▶ [STAGE 4/4] Calculating Code Coverage for etl/ Package...${NC}"
if $PYTEST tests/ -v --cov=etl --cov-report=term-missing; then
    echo -e "${GREEN}✔ [STAGE 4] Code coverage report generated successfully.${NC}"
else
    echo -e "${RED}✘ [STAGE 4] Test coverage execution failed.${NC}"
    FAILED_STAGES+=("Stage 4: Code Coverage")
fi

# ------------------------------------------------------------------------------
# FINAL REPORT
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}${BLUE}==============================================================================${NC}"
if [ ${#FAILED_STAGES[@]} -eq 0 ]; then
    echo -e "${BOLD}${GREEN}✔ ALL CI TEST GATES PASSED! Pipeline is verified and ready for deployment.${NC}"
    echo -e "${BOLD}${BLUE}==============================================================================${NC}"
    exit 0
else
    echo -e "${BOLD}${RED}✘ CI RUN FAILED in the following stage(s):${NC}"
    for stage in "${FAILED_STAGES[@]}"; do
        echo -e "   ${RED}- $stage${NC}"
    done
    echo -e "${BOLD}${BLUE}==============================================================================${NC}"
    exit 1
fi

