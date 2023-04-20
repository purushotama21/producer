import warnings
warnings.filterwarnings('ignore')
import sys
import psycopg2 as pg
import os
from io import open
import pandas as pd

db_files = set()
datashare =set()

def addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/database/') > 0:
			db_files.add(commit_item)
		elif commit_item.find('/datashare/') > 0:
			datashare.add(commit_item)
def db_creation(filenames,conn,cur):
    try:
        df = pd.read_sql('select src_env_qa from data_gov.em_db_map where src_env_qa is not null;', conn)
        pg_databases = pd.read_sql('select datname as dbname from pg_database;',conn)
        map_dbs = df['src_env_qa'].tolist()
        pg_dbs = list(pg_databases['dbname'])
        print("pg_dbs : ",pg_dbs)
        print("map_dbs:",map_dbs)
        for filename in filenames:
            # Specify encoding since GitHub puts garbage characters at beginning of file
            if os.path.exists(filename):
                with open(filename,'r') as file:
                    lines=file.readline()
                    db=(lines.split()[2]).replace(';','')
                    print('words in a mapping db: ',db)
                if (db in map_dbs) and (db not in pg_dbs):
                    print(F'creating database {db}')
                    cur.execute(F'create database {db}')
                    cur.execute(F'GRANT ALL PRIVILEGES ON DATABASE {db} TO admin;')
                else:
                    print('database is not in mapping table')   

    except Exception as e:
        print(e)

def main():
    print("Running RS Deployer...")
    conn = None
    cur = None
    try:
        if len(sys.argv) != 7 or len(sys.argv[6]) < 1:
            raise Exception("Invalid command line arguments. Expected <host> <database> <port> <username> <password> <commit_id> where <commit_id> is the hash for the most \\n recent commit. Note that this error may also appear if you do not have any tags on your project. You need at least one to start off.")
        # Build lists of SQL objects to be deployed. Split the new commit changes
        # We are now parsing a temp file due to a limitation on the # of characters that
        # can be passed via sys.argv. This was an issue on large commits.
        #git_commit_list = ['.' + os.sep + x for x in sys.argv[6].splitlines()]
        commit_file = "/tmp/git_diff_files_" + sys.argv[6]
        git_commit_list = []
        with open(commit_file) as f:
            for line in f.read().splitlines():
                git_commit_list.append('.' + os.sep + line)
        print(git_commit_list)

        #list_file = 'SQL/rs_utils/post_deployment/BUILD_RS_DB_OBJECTS.LIST'
        list_file = 'BUILD_RS_DB_OBJECTS'
        read_list_file = True
        for commit_item in git_commit_list:
            if os.path.isfile(commit_item) == False:
                print("File could not be found: " + commit_item + ". Skipping...")
            if os.path.isfile(commit_item) == True:
                addCommitedFile(commit_item)
            else:
                print("File not found: " +  commit_item)
        if len(db_files) > 0 or len(datashare) > 0:
            conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
            print("connection established successfully")
            cur = conn.cursor()
            conn.autocommit = True
            print("Deploying Objects...")
            if len(db_files) > 0:
                db_creation(db_files,conn,cur)
            if len(datashare) > 0:
                 db_creation(datashare,conn,cur)
            conn.commit()
            print('-----------------------------------------------------------------------')
            print('---------------------- Deployment Summary Report ----------------------')
            print('-----------------------------------------------------------------------')
            if len(db_files) > 0:
                print("Total number of databases : "+str(len(db_files)))
            if len(datashare) > 0:
                print("Total number of datashare: "+str(len(datashare)))
            print('-----------------------------------------------------------------------')
        else:
            print("No valid files found to deploy in <files_to_deploy>: " + commit_file)            
    except Exception as e:
        print("DEPLOYER ERROR: " + getattr(e, 'strerror', str(e)))
        if cur is not None:
            conn.rollback()
        sys.exit(200)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

if __name__ == '__main__':
        main()
