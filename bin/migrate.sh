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
HOST="127.0.0.1"
for ((i=1;i<=$#;i++));
do
    if [ ${!i} = "-h" ]
    then ((i++))
        HOST=${!i};

    elif [ ${!i} = "-p" ]
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

echo -e "${YELLOW}************************${NC}"
echo -e "${YELLOW} MIGRATION MYSQL > PG   ${NC}"
echo -e "${YELLOW}************************${NC}"

# touch ${DUMPLOG}
# echo -e "${YELLOW}Logs are being redirect to:${NC} \n\tQueries:\t${DUMPLOG}\n\tErrors:\t\t${ERRLOG}${NC}"

echo -en "${YELLOW}Creating db${NC}..."
psql --set ON_ERROR_STOP=on -h ${HOST} -U ${USER} -p ${PORT} -o ${ERRLOG} -d postgres -c "DROP DATABASE IF EXISTS \"${DB_NAME}\"" || error "Dropping old db"
psql --set ON_ERROR_STOP=on -h ${HOST} -U ${USER} -p ${PORT} -o ${ERRLOG} -d postgres -c "CREATE DATABASE \"${DB_NAME}\"" || error "Creating db"
echo "Done ($SECONDS)"

SECONDS=0
SQL="./output/${DB_NAME}/psql_tables.sql"
echo -en "${YELLOW}Creating v2 tables${NC}..."
psql --set ON_ERROR_STOP=on -h ${HOST} -U ${USER} -p ${PORT} -f ${SQL} -o ${ERRLOG} -d ${DB_NAME} || error "Creating tables"
echo "Done ($SECONDS)"

SECONDS=0
if [ ${DB_NAME} = "blpcore" ]
then
  cat -v ./output/blpcore/tables/program.sql | sed "s/'\^@'/0/g" > ./output/blpcore/tables/program.sql.new
  mv ./output/blpcore/tables/program.sql.new ./output/blpcore/tables/program.sql
  cat -v ./output/blpcore/tables/program.sql | sed "s/'\^A'/1/g" > ./output/blpcore/tables/program.sql.new
  mv ./output/blpcore/tables/program.sql.new ./output/blpcore/tables/program.sql
  cat -v ./output/blpcore/tables/program_aud.sql | sed "s/'\^@'/0/g" > ./output/blpcore/tables/program_aud.sql.new
  mv ./output/blpcore/tables/program_aud.sql.new ./output/blpcore/tables/program_aud.sql
  cat -v ./output/blpcore/tables/program_aud.sql | sed "s/'\^A'/1/g" > ./output/blpcore/tables/program_aud.sql.new
  mv ./output/blpcore/tables/program_aud.sql.new ./output/blpcore/tables/program_aud.sql
fi
SQL="./output/${DB_NAME}/psql_data.sql"
echo -en "${YELLOW}Inserting data${NC}..."
psql --set  ON_ERROR_STOP=on -h ${HOST} -U ${USER} -p ${PORT} -f ${SQL} -o ${ERRLOG} -d ${DB_NAME} || error "Inserting data"
echo "Done ($SECONDS)"

SECONDS=0
SQL="./output/${DB_NAME}/psql_views.sql"
echo -en "${YELLOW}Creating views${NC}..."
psql --set ON_ERROR_STOP=on -h ${HOST} -U ${USER} -p ${PORT} -f ${SQL} -o ${ERRLOG} -d ${DB_NAME} || error "Creating views"
echo "Done ($SECONDS)"

SECONDS=0
if [ ${DB_NAME} = "blpcore" ]
then
  cat -v ./output/blpcore/psql_index_fk.sql | sed 's/REFERENCES user/REFERENCES "blp_user"/g' > ./output/blpcore/psql_index_fk.sql.new
  mv ./output/blpcore/psql_index_fk.sql.new ./output/blpcore/psql_index_fk.sql
fi
SQL="./output/${DB_NAME}/psql_index_fk.sql"
echo -en "${YELLOW}Creating indexes and fk${NC}..."
psql --set ON_ERROR_STOP=on -h ${HOST} -U ${USER} -p ${PORT} -f ${SQL} -o ${ERRLOG} -d ${DB_NAME} || error "Creating add indexes and constraints"
echo "Done ($SECONDS)"

echo -e "${GREEN}Migration to PG was completed SUCCESSFULLY${NC}"
exit 0
