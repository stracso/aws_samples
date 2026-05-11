#!/usr/bin/env bash
set -euo pipefail

# Setup script for AWS Glue Interactive Sessions with PyCharm Jupyter
# Prerequisites: Python 3.8+, pip, AWS CLI configured with appropriate credentials

JUPYTER_PORT="${JUPYTER_PORT:-8888}"
GLUE_VERSION="${GLUE_VERSION:-5.0}"

echo "=== AWS Glue Interactive Session Setup ==="

# 1. Install aws-glue-sessions and jupyter
echo ""
echo "[1/4] Installing aws-glue-sessions and jupyter..."
pip install --upgrade aws-glue-sessions jupyter

# 2. Install Glue Jupyter kernels
echo ""
echo "[2/4] Installing Glue Jupyter kernels..."
install-glue-kernels

# 3. Verify kernels are installed
echo ""
echo "[3/4] Verifying installed kernels..."
jupyter kernelspec list | grep -i glue || {
    echo "ERROR: Glue kernels not found. Try running: install-glue-kernels"
    exit 1
}

# 4. Print connection instructions
echo ""
echo "[4/4] Setup complete!"
echo ""
echo "==========================================="
echo "  HOW TO CONNECT FROM PYCHARM"
echo "==========================================="
echo ""
echo "1. Start the local Jupyter server:"
echo ""
echo "   jupyter notebook --no-browser --port=${JUPYTER_PORT}"
echo ""
echo "   (Copy the URL with token from the output)"
echo ""
echo "2. In PyCharm Professional:"
echo "   - Open or create a .ipynb file"
echo "   - Click 'Jupyter Server' dropdown at the top"
echo "   - Select 'Configured Server'"
echo "   - Paste: http://localhost:${JUPYTER_PORT}/?token=<your-token>"
echo "   - Select kernel: 'Glue PySpark' or 'Glue Spark'"
echo ""
echo "3. In the first notebook cell, configure your session:"
echo ""
echo "   %iam_role arn:aws:iam::<ACCOUNT_ID>:role/<GLUE_ROLE>"
echo "   %region <AWS_REGION>"
echo "   %worker_type G.1X"
echo "   %number_of_workers 2"
echo "   %idle_timeout 60"
echo "   %glue_version ${GLUE_VERSION}"
echo ""
echo "==========================================="
echo ""
echo "To start the Jupyter server now, run:"
echo "  jupyter notebook --no-browser --port=${JUPYTER_PORT}"