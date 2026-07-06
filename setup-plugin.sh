#
# Run this script to setup your environment
#

python3 -m qgis > /dev/null && {
    # Install uv
    which uv >/dev/null || {
        echo "Installing uv";
        curl -LsSf https://astral.sh/uv/install.sh | sh;
    }

    echo "Qgis seems to be installed, creating venv locally with system packages"
    uv venv --system-site-packages
    echo "Updating environment"
    make sync
} || {
    echo "Qgis is not installed locally, not installing environment"
}


echo "Done..."
