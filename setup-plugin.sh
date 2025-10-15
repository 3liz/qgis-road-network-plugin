#
# Run this script whenever you customize the plugin
# to your needs
#

# Install uv
which uv >/dev/null || { 
    echo "Installing uv";
    curl -LsSf https://astral.sh/uv/install.sh | sh; 
}

python3 -m qgis > /dev/null && {
    echo "Qgis seems to be installed, creating venv locally with system packages"
    uv venv --system-site-packages
}

echo "Updating dependencies"
make update-dependencies

if [ ! -e .localconfig.mk ]; then
    echo "USE_UV=1" > .localconfig.mk
else
    echo "Hint: add 'USE_UV=1' to your .localconfig.mk"
fi

echo "Done..."
