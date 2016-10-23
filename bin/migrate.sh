#!/usr/bin/env bash
readonly YELLOW="\033[0;33m"
readonly RED="\033[0;31m"
readonly GREEN="\033[0;32m"
readonly NC="\033[0;0m"
readonly BASH_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function error {
    MSG="$1"
    echo -e "${RED}ERROR: ${MSG}${NC}\nExitting"
    exit 1
}

PORT=5432
for ((i=1;i<=$#;i++));
do
    if [ ${!i} = "-p" ]
    then ((i++))
        PORT=${!i};

    elif [ ${!i} = "-d" ];
    then ((i++))
        DB_NAME=${!i};

    elif [ ${!i} = "-U" ];
    then ((i++))
        USER=${!i};

    elif [ ${!i} = "-Wf" ];
    then ((i++))
        export PGPASSWORD=${!i};

    elif [ ${!i} = "-W" ];
    then ((i++))
        echo -en "${YELLOW}Root password${NC}\n"
        read -s password
        export PGPASSWORD=$password
    fi
done;

readonly ERRLOG="/tmp/pg_migration_$(date +%s).err"
touch "${ERRLOG}"
readonly HOST="127.0.0.1"

echo -e "${YELLOW}************************${NC}"
echo -e "${YELLOW} MIGRATION MYSQL > PG   ${NC}"
echo -e "${YELLOW}************************${NC}"

# touch ${DUMPLOG}
# echo -e "${YELLOW}Logs are being redirect to:${NC} \n\tQueries:\t${DUMPLOG}\n\tErrors:\t\t${ERRLOG}${NC}"

echo -en "${YELLOW}Creating db${NC}..."
psql --set ON_ERROR_STOP=on -U ${USER} -p ${PORT} -o ${ERRLOG} -c "DROP DATABASE IF EXISTS ${DB_NAME}" || error "Dropping old db"
psql --set ON_ERROR_STOP=on -U ${USER} -p ${PORT} -o ${ERRLOG} -c "CREATE DATABASE ${DB_NAME}" || error "Creating db"
echo "Done ($SECONDS)"

SECONDS=0
SQL="./output/${DB_NAME}/psql_tables.sql"
echo -en "${YELLOW}Creating v2 tables${NC}..."
psql --set ON_ERROR_STOP=on -U ${USER} -p ${PORT} -f ${SQL} -o ${ERRLOG} -d ${DB_NAME} || error "Creating tables"
echo "Done ($SECONDS)"

SECONDS=0
SQL="./output/${DB_NAME}/psql_data.sql"
echo -en "${YELLOW}Inserting data${NC}..."
psql --set  ON_ERROR_STOP=on -U ${USER} -p ${PORT} -f ${SQL} -o ${ERRLOG} -d ${DB_NAME} || error "Inserting data"
echo "Done ($SECONDS)"

SECONDS=0
SQL="./output/${DB_NAME}/psql_views.sql"
echo -en "${YELLOW}Creating views${NC}..."
psql --set ON_ERROR_STOP=on -U ${USER} -p ${PORT} -f ${SQL} -o ${ERRLOG} -d ${DB_NAME} || error "Creating views"
echo "Done ($SECONDS)"

SECONDS=0
SQL="./output/${DB_NAME}/psql_index_fk.sql"
echo -en "${YELLOW}Creating indexes and fk${NC}..."
psql --set ON_ERROR_STOP=on -U ${USER} -p ${PORT} -f ${SQL} -o ${ERRLOG} -d ${DB_NAME} || error "Creating add indexes and constraints"
echo "Done ($SECONDS)"

echo -e "${GREEN}Migration to PG was completed SUCCESSFULLY${NC}"
exit 0
