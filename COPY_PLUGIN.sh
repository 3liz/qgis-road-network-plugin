# HOW TO
# * You must copy the directory containing this file in a new place
# * Then edit it and change the variables
# * Then run it with ./COPY_PLUGIN.sh

# The value to use for the new plugin
# You must change them
PLUGIN_DIR_NAME=roadnetwork
PLUGIN_CLASS_NAME=RoadNetwork
PLUGIN_PG_SCHEMA=road_graph
PLUGIN_PG_SERVICE=pg_road_network_service
PLUGIN_GITHUB_REPOSITORY=qgis-road-network-plugin
PLUGIN_TRANSIFEX_KEY=road-network

# The value inside the example plugin
# ######### DO NOT CHANGE ###########
SRC_PLUGIN_DIR_NAME=lizexample
SRC_PLUGIN_CLASS_NAME=LizExample
SRC_PLUGIN_PG_SCHEMA=liz_example_pg_schema
SRC_PLUGIN_PG_SERVICE=lizservice
SRC_PLUGIN_GITHUB_REPOSITORY=qgis-liz-example-plugin
SRC_PLUGIN_TRANSIFEX_KEY=liz-example

# PostgreSQL schema
mv "$SRC_PLUGIN_DIR_NAME"/install/sql/"$SRC_PLUGIN_PG_SCHEMA" "$SRC_PLUGIN_DIR_NAME"/install/sql/"$PLUGIN_PG_SCHEMA"
mv "$SRC_PLUGIN_DIR_NAME"/test/data/install/sql/"$SRC_PLUGIN_PG_SCHEMA" "$SRC_PLUGIN_DIR_NAME"/test/data/install/sql/"$PLUGIN_PG_SCHEMA"
rgrep $SRC_PLUGIN_PG_SCHEMA -l | grep -v COPY_PLUGIN | xargs sed -i "s/"$SRC_PLUGIN_PG_SCHEMA"/"$PLUGIN_PG_SCHEMA"/g"

# Class name
rgrep $SRC_PLUGIN_CLASS_NAME -l | grep -v COPY_PLUGIN | xargs sed -i "s/"$SRC_PLUGIN_CLASS_NAME"/"$PLUGIN_CLASS_NAME"/g"

# Github repo
rgrep $SRC_PLUGIN_GITHUB_REPOSITORY -l | grep -v COPY_PLUGIN | xargs sed -i "s/"$SRC_PLUGIN_GITHUB_REPOSITORY"/"$PLUGIN_GITHUB_REPOSITORY"/g"
sed -i "s/"$SRC_PLUGIN_TRANSIFEX_KEY"/"$PLUGIN_TRANSIFEX_KEY"/g" setup.cfg

# service
rgrep $SRC_PLUGIN_PG_SERVICE -l | grep -v COPY_PLUGIN | xargs sed -i "s/"$SRC_PLUGIN_PG_SERVICE"/"$PLUGIN_PG_SERVICE"/g"

# Python import
rgrep "from "$SRC_PLUGIN_DIR_NAME"." -l | grep -v COPY_PLUGIN | xargs sed -i "s/from "$SRC_PLUGIN_DIR_NAME"./from "$PLUGIN_DIR_NAME"./g"

# Plugin name and underscore
rgrep "$SRC_PLUGIN_DIR_NAME"_ -l | grep -v COPY_PLUGIN | xargs sed -i "s/"$SRC_PLUGIN_DIR_NAME"_/"$PLUGIN_DIR_NAME"_/g"

# Last replacement
rgrep $SRC_PLUGIN_DIR_NAME -l | grep -v COPY_PLUGIN | xargs sed -i "s/"$SRC_PLUGIN_DIR_NAME"/"$PLUGIN_DIR_NAME"/g"

# files
mv $SRC_PLUGIN_DIR_NAME/"$SRC_PLUGIN_DIR_NAME"_dockwidget.py     $SRC_PLUGIN_DIR_NAME/"$PLUGIN_DIR_NAME"_dockwidget.py
mv $SRC_PLUGIN_DIR_NAME/"$SRC_PLUGIN_DIR_NAME".py $SRC_PLUGIN_DIR_NAME/"$PLUGIN_DIR_NAME".py
mv $SRC_PLUGIN_DIR_NAME/resources/ui/"$SRC_PLUGIN_DIR_NAME"_dockwidget_base.ui $SRC_PLUGIN_DIR_NAME/resources/ui/"$PLUGIN_DIR_NAME"_dockwidget_base.ui
mv $SRC_PLUGIN_DIR_NAME/resources/qgis/"$SRC_PLUGIN_DIR_NAME"_administration.qgs $SRC_PLUGIN_DIR_NAME/resources/qgis/"$PLUGIN_DIR_NAME"_administration.qgs
mv docs/media/"$SRC_PLUGIN_DIR_NAME".svg docs/media/"$PLUGIN_DIR_NAME".svg

# directory
mv $SRC_PLUGIN_DIR_NAME $PLUGIN_DIR_NAME

# Git
git init --initial-branch=main
git submodule add https://github.com/3liz/qgis_plugin_tools.git "$PLUGIN_DIR_NAME"/qgis_plugin_tools
git add --all
git commit -m "Initial commit"
git log
git remote add hub https://github.com/3liz/$PLUGIN_GITHUB_REPOSITORY
