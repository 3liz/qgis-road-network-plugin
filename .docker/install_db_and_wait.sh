set -e

./install_db.sh
touch /tmp/.db-runner-ok
sleep infinity
