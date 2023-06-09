import sys
import psycopg2 as pg
import os
from io import open

###################################################################################################
# RedShift Deployment Tool
###################################################################################################
# - This script runs the supplied SQL script files (.sql) pointed at the supplied database.
# - Exits with code 0 upon success, 200 on error.
# - Scripts must be organized in the one of the following directories in order to assure they are
#	deployed in the appropriate order:
#	Under SQL -> 
#		-> {schema}
#			-> pre_deployment
#			-> tables
#			-> views
#			-> lbviews (late-binding views)
#			-> functions
#			-> procedures
#			-> constraints
#			-> post_deployment

# - Deployment happens within a single transaction.  Everything will rollback completely upon failure.
###################################################################################################

cp_pre_deployment = set()
cp_post_deployment = set()
cp_tables = set()
cp_views = set()
cp_procedures = set()

cobra_pre_deployment = set()
cobra_post_deployment = set()
cobra_tables = set()
cobra_views = set()
cobra_procedures = set()

ppts_pre_deployment = set()
ppts_post_deployment = set()
ppts_tables = set()
ppts_views = set()
ppts_procedures = set()

khw_pre_deployment = set()
khw_post_deployment = set()
khw_tables = set()
khw_views = set()
khw_procedures = set()

golf_pre_deployment = set()
golf_post_deployment = set()
golf_tables = set()
golf_views = set()
golf_procedures = set()


sss_pre_deployment = set()
sss_post_deployment = set()
sss_tables = set()
sss_views = set()
sss_procedures = set()

jderpt_pre_deployment = set()
jderpt_post_deployment = set()
jderpt_tables = set()
jderpt_views = set()
jderpt_procedures = set()

crpdta_pre_deployment = set()
crpdta_post_deployment = set()
crpdta_tables = set()
crpdta_views = set()
crpdta_procedures = set()

impact_pre_deployment = set()
impact_post_deployment = set()
impact_tables = set()
impact_views = set()
impact_procedures = set()

crd_pre_deployment = set()
crd_post_deployment = set()
crd_tables = set()
crd_views = set()
crd_procedures = set()


kronos_pre_deployment = set()
kronos_post_deployment = set()
kronos_tables = set()
kronos_views = set()
kronos_procedures = set()

ppm_niku_pre_deployment = set()
ppm_niku_post_deployment = set()
ppm_niku_tables = set()
ppm_niku_views = set()
ppm_niku_procedures = set()

okc_pre_deployment = set()
okc_post_deployment = set()
okc_tables = set()
okc_views = set()
okc_procedures = set()

wfm_cloudwfr_pre_deployment = set()
wfm_cloudwfr_post_deployment = set()
wfm_cloudwfr_tables = set()
wfm_cloudwfr_views = set()
wfm_cloudwfr_procedures = set()

mytime_dbo_pre_deployment = set()
mytime_dbo_post_deployment = set()
mytime_dbo_tables = set()
mytime_dbo_views = set()
mytime_dbo_procedures = set()

aps_msc_pre_deployment = set()
aps_msc_post_deployment = set()
aps_msc_tables = set()
aps_msc_views = set()
aps_msc_procedures = set()

drm_pre_deployment = set()
drm_post_deployment = set()
drm_tables = set()
drm_views = set()
drm_procedures = set()




func_owner_change_sql = '''
select nsp.nspname||'.'||p.proname||'('||oidvectortypes(p.proargtypes)||') owner to ' as dyna_sql
from pg_proc p
join pg_namespace nsp ON p.pronamespace = nsp.oid
where nsp.nspname = %s and p.proname = %s;
'''

def takeTableBackup(filenames, cursor):
	for filename in filenames:
		schema_name, tbl_name = extract_obj_name(filename)
		print("Check and backup the table data: " + schema_name + '.' + tbl_name)
		cursor.execute('call rs_utils.recreate_table_with_data(%s, %s, true)', (schema_name, tbl_name))
		
def restoreTableData(filenames, cursor):
	for filename in filenames:
		schema_name, tbl_name = extract_obj_name(filename)
		print("Check and backup the table data: " + schema_name + '.' + tbl_name)
		cursor.execute('call rs_utils.restore_table_with_data(%s, %s)', (schema_name, tbl_name))

def extract_obj_name(filename):
	try:
		eles=filename.split('/')
		obj_name = None
		if len(eles) > 5:
			obj_name=eles[5:]
		else:
			obj_name=eles[4:]
		obj_full_name = obj_name[0].split('.')
		print(obj_full_name[0],obj_full_name[1])
		return obj_full_name[0], obj_full_name[1]
	except Exception as e:
		print('Unable to fetch schema name for # ' +filename)
	return '', ''

def generate_owner_change_sql(cursor, schema_name, obj_name, object_type, new_owner):
	cursor.execute(func_owner_change_sql,(schema_name, obj_name))
	obj_list = cursor.fetchall()
	sql_list = list(set([x[0] for x in obj_list]))
	final_sql = ""
	for sql in sql_list:
		final_sql = final_sql + 'alter ' + object_type + ' '+ sql + new_owner +'; '
	return final_sql

def writeToDB(filenames, cursor, obj_type, schema_owner_map, update_owner):

	for filename in filenames:
		# Specify encoding since GitHub puts garbage characters at beginning of file
		if os.path.exists(filename):
			with open(filename, 'r') as sqlFile:
				print("Attempting to deploy: " + filename)
				sqlContent = sqlFile.readlines()

				if len(''.join(sqlContent).strip()) > 0:
					cursor.execute(''.join(sqlContent))
					schema_name, obj_name = extract_obj_name(filename)
					if update_owner == True and schema_name in schema_owner_map:
						if (obj_type == 'table' or obj_type == 'view') and filename.find('default_late_binding_views') < 0:
							cursor.execute('alter '+obj_type+' '+schema_name+'.'+obj_name+' owner to '+schema_owner_map[schema_name])
						elif (obj_type == 'function' or obj_type == 'procedure'):
							function_owner_change_sqls = generate_owner_change_sql(cursor, schema_name, obj_name, obj_type, schema_owner_map[schema_name])
							cursor.execute(function_owner_change_sqls)
					print("Successfully deployed: " + filename)
				else:
					print("Empty File: " + filename)
		else:
			print("File Not Found : " + filename)


def addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			cp_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			cp_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			cp_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			cp_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in cp_tables:
				cp_tables.add(commit_item)
#cobra_addCommitedFile
def cobra_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			cobra_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			cobra_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			cobra_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			cobra_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in cobra_tables:
				cobra_tables.add(commit_item)
#ppts_addCommitedFile
def ppts_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			ppts_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			ppts_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			ppts_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			ppts_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in ppts_tables:
				ppts_tables.add(commit_item)
#khw_addCommitedFile
def khw_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			khw_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			khw_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			khw_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			khw_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in khw_tables:
				khw_tables.add(commit_item)
#golf_addCommitedFile
def golf_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			golf_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			golf_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			golf_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			golf_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in golf_tables:
				golf_tables.add(commit_item)
#sss_addCommitedFile
def sss_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			sss_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			sss_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			sss_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			sss_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in sss_tables:
				sss_tables.add(commit_item)
#jderpt_addCommitedFile
def jderpt_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			jderpt_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			jderpt_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			jderpt_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			jderpt_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in jderpt_tables:
				jderpt_tables.add(commit_item)
#crpdta_addCommitedFile
def crpdta_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			crpdta_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			crpdta_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			crpdta_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			crpdta_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in crpdta_tables:
				crpdta_tables.add(commit_item)
#impact_addCommitedFile
def impact_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			impact_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			impact_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			impact_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			impact_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in impact_tables:
				impact_tables.add(commit_item)

#crd_addCommitedFile
def crd_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			crd_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			crd_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			crd_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			crd_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in crd_tables:
				crd_tables.add(commit_item)
#kronos_addCommitedFile
def kronos_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			kronos_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			kronos_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			kronos_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			kronos_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in kronos_tables:
				kronos_tables.add(commit_item)
#ppm_niku_addCommitedFile
def ppm_niku_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			ppm_niku_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			ppm_niku_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			ppm_niku_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			ppm_niku_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in ppm_niku_tables:
				ppm_niku_tables.add(commit_item)

#okc_addCommitedFile
def okc_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			okc_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			okc_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			okc_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			okc_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in okc_tables:
				okc_tables.add(commit_item)

#wfm_cloudwfr_addCommitedFile
def wfm_cloudwfr_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			wfm_cloudwfr_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			wfm_cloudwfr_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			wfm_cloudwfr_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			wfm_cloudwfr_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in wfm_cloudwfr_tables:
				wfm_cloudwfr_tables.add(commit_item)
#mytime_dbo_addCommitedFile
def mytime_dbo_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			mytime_dbo_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			mytime_dbo_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			mytime_dbo_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			mytime_dbo_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in mytime_dbo_tables:
				mytime_dbo_tables.add(commit_item)

#aps_msc_addCommitedFile
def aps_msc_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			aps_msc_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			aps_msc_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			aps_msc_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			aps_msc_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in aps_msc_tables:
				aps_msc_tables.add(commit_item)
#drm_addCommitedFile
def drm_addCommitedFile(commit_item):
	if commit_item.endswith('.sql'):
		if commit_item.find('/pre_deployment/') > 0:
			drm_pre_deployment.add(commit_item)
		# elif commit_item.find('/functions/') > 0:
		# 	cp_functions.add(commit_item)
		elif commit_item.find('/procedures/') > 0:
			drm_procedures.add(commit_item)
		elif commit_item.find('/views/') > 0:
			drm_views.add(commit_item)
		# elif commit_item.find('/lbviews/') > 0:
		# 	cp_lbviews.add(commit_item)
		# elif commit_item.find('/constraints/') > 0:
		# 	cp_constraints.add(commit_item)
		elif commit_item.find('/post_deployment/') > 0:
			drm_post_deployment.add(commit_item)

	if commit_item.endswith('.ddl'):
		if commit_item.find('/tables/') > 0 and commit_item not in drm_tables:
				drm_tables.add(commit_item)

def fetch_schema_owners(cursor, build_fsso):
	if build_fsso == '502825978':	# Required only for Central CICD Job, other jobs uses their own FSSO to build code
		cursor.execute('select rs_schema, owner from rs_utils.schema_owner_mapping where active = true')
		result1 = cursor.fetchall()
		schema_owner_map = dict([(x[0],'"'+x[1]+'"') for x in result1])
	else:
		print("Skipped ownership change as it is not central CICD job")
		schema_owner_map = {}
	return schema_owner_map

def main():

	print("Running RS Deployer...")

	cp_conn = None
	cp_cur = None
	cobra_conn = None
	cobra_cur = None
	ppts_conn=None
	ppts_cur=None
	khw_conn=None
	khw_cur=None
	golf_conn = None
	golf_cur = None
	sss_conn = None
	sss_cur = None
	jderpt_conn = None
	jderpt_cur = None
	crpdta_conn = None
	crpdta_cur = None
	impact_conn = None
	impact_cur = None
	crd_conn = None
	crd_cur = None
	kronos_conn = None
	kronos_cur = None
	ppm_niku_conn = None
	ppm_niku_cur = None
	okc_conn = None
	okc_cur = None
	wfm_cloudwfr_conn = None
	wfm_cloudwfr_cur = None
	mytime_dbo_conn = None
	mytime_dbo_cur = None
	aps_msc_conn = None
	aps_msc_cur = None
	drm_conn = None
	drm_cur = None


	try:

		# Argument validation: make sure the correct number of arguments was supplied
		if len(sys.argv) != 9 or len(sys.argv[6]) < 1:
			raise Exception("Invalid command line arguments. Expected <host> <database> <port> <username> <password> <commit_id> where <commit_id> is the hash for the most \\n recent commit. Note that this error may also appear if you do not have any tags on your project. You need at least one to start off.")


		# Build lists of SQL objects to be deployed. Split the new commit changes
		# UPDATE 5/2021: We are now parsing a temp file due to a limitation on the # of characters that 
		# can be passed via sys.argv. This was an issue on large commits.
		#git_commit_list = ['.' + os.sep + x for x in sys.argv[6].splitlines()]
		commit_file = "/tmp/git_diff_files_" + sys.argv[6]
		git_commit_list = []	
		with open(commit_file) as f:	
			for line in f.read().splitlines():
				git_commit_list.append('.' + os.sep + line)			  								
		print(git_commit_list)
		lst_centrepiece = []
		lst_cobra = []
		lst_ppts = []
		lst_khw = []
		lst_golf = []
		lst_sss = []
		lst_jderpt = []
		lst_crpdta =  []
		lst_impact = []
		lst_crd = []
		lst_kronos = []
		lst_ppm_niku = []
		lst_okc = []
		lst_wfm_cloudwfr = []
		lst_mytime_dbo = []
		lst_aps_msc = []
		lst_drm = []


		for commit_item in git_commit_list:
			if os.path.isfile(commit_item) == False:
				print("File could not be found: " + commit_item + ". Skipping...")
			if os.path.isfile(commit_item) == True:
				if commit_item.endswith('.sql'):
					if commit_item.find('/centrepiece/') > 0:
						lst_centrepiece.append(commit_item)
					elif commit_item.find('/cobra/') > 0:
						lst_cobra.append(commit_item)
					elif commit_item.find('/ppts/') > 0:
						lst_ppts.append(commit_item)
					elif commit_item.find('/khw/') > 0:
						lst_khw.append(commit_item)
					elif commit_item.find('/golf/') > 0:
						lst_golf.append(commit_item)
					elif commit_item.find('/sss/') > 0:
						lst_sss.append(commit_item)
					elif commit_item.find('/jderpt/') > 0:
						lst_jderpt.append(commit_item)
					elif commit_item.find('/crpdta/') > 0:
						lst_crpdta.append(commit_item)
					elif commit_item.find('/impact/') > 0:
						lst_impact.append(commit_item)
					elif commit_item.find('/crd/') > 0:
						lst_crd.append(commit_item)
					elif commit_item.find('/kronos/') > 0:
						lst_kronos.append(commit_item)
					elif commit_item.find('/ppm_niku/') > 0:
						lst_ppm_niku.append(commit_item)
					elif commit_item.find('/okc/') > 0:
						lst_okc.append(commit_item)
					elif commit_item.find('/wfm_cloudwfr/') > 0:
						lst_wfm_cloudwfr.append(commit_item)
					elif commit_item.find('/mytime_dbo/') > 0:
						lst_mytime_dbo.append(commit_item)
					elif commit_item.find('/aps_msc/') > 0:
						lst_aps_msc.append(commit_item)
					elif commit_item.find('/drm/') > 0:
						lst_drm.append(commit_item)                
				if commit_item.endswith('.ddl'):
					if commit_item.find('/centrepiece/') > 0:
						lst_centrepiece.append(commit_item)
					elif commit_item.find('/cobra/') > 0:
						lst_cobra.append(commit_item)
					elif commit_item.find('/ppts/') > 0:
						lst_ppts.append(commit_item)
					elif commit_item.find('/khw/') > 0:
						lst_khw.append(commit_item)
					elif commit_item.find('/golf/') > 0:
						lst_golf.append(commit_item)
					elif commit_item.find('/sss/') > 0:
						lst_sss.append(commit_item)
					elif commit_item.find('/jderpt/') > 0:
						lst_jderpt.append(commit_item)
					elif commit_item.find('/crpdta/') > 0:
						lst_crpdta.append(commit_item)
					elif commit_item.find('/impact/') > 0:
						lst_impact.append(commit_item)
					elif commit_item.find('/crd/') > 0:
						lst_crd.append(commit_item)
					elif commit_item.find('/kronos/') > 0:
						lst_kronos.append(commit_item)
					elif commit_item.find('/ppm_niku/') > 0:
						lst_ppm_niku.append(commit_item)
					elif commit_item.find('/okc/') > 0:
						lst_okc.append(commit_item)
					elif commit_item.find('/wfm_cloudwfr/') > 0:
						lst_wfm_cloudwfr.append(commit_item)
					elif commit_item.find('/mytime_dbo/') > 0:
						lst_mytime_dbo.append(commit_item)
					elif commit_item.find('/aps_msc/') > 0:
						lst_aps_msc.append(commit_item)
					elif commit_item.find('/drm/') > 0:
						lst_drm.append(commit_item)
			else:
				print("File not found: " +  commit_item)
		
		#lst_centrepiece
		for cp_commit_file in lst_centrepiece:
			if os.path.isfile(cp_commit_file) == False:
				print("File could not be found: " + cp_commit_file + ". Skipping...")
			if os.path.isfile(cp_commit_file) == True:
				addCommitedFile(cp_commit_file)
			else:
				print("File not found: " +  cp_commit_file)
		#lst_cobra
		for cobra_commit_file in lst_cobra:
			if os.path.isfile(cobra_commit_file) == False:
				print("File could not be found: " + cobra_commit_file + ". Skipping...")
			if os.path.isfile(cobra_commit_file) == True:
				cobra_addCommitedFile(cobra_commit_file)
			else:
				print("File not found: " +  cobra_commit_file)
		#lst_ppts
		for ppts_commit_file in lst_ppts:
			if os.path.isfile(ppts_commit_file) == False:
				print("File could not be found: " + ppts_commit_file + ". Skipping...")
			if os.path.isfile(ppts_commit_file) == True:
				ppts_addCommitedFile(ppts_commit_file)
			else:
				print("File not found: " +  ppts_commit_file)		
		#lst_khw
		for khw_commit_file in lst_khw:
			if os.path.isfile(khw_commit_file) == False:
				print("File could not be found: " + khw_commit_file + ". Skipping...")
			if os.path.isfile(khw_commit_file) == True:
				khw_addCommitedFile(khw_commit_file)
			else:
				print("File not found: " +  khw_commit_file)
		#lst_golf					
		for golf_commit_file in lst_golf:
			if os.path.isfile(golf_commit_file) == False:
				print("File could not be found: " + golf_commit_file + ". Skipping...")
			if os.path.isfile(golf_commit_file) == True:
				golf_addCommitedFile(golf_commit_file)
			else:
				print("File not found: " +  golf_commit_file)
		#lst_sss
		for sss_commit_file in lst_sss:
			if os.path.isfile(sss_commit_file) == False:
				print("File could not be found: " + sss_commit_file + ". Skipping...")
			if os.path.isfile(sss_commit_file) == True:
				sss_addCommitedFile(sss_commit_file)
			else:
				print("File not found: " +  sss_commit_file)
		#lst_jderpt				
		for jderpt_commit_file in lst_jderpt:
			if os.path.isfile(jderpt_commit_file) == False:
				print("File could not be found: " + jderpt_commit_file + ". Skipping...")
			if os.path.isfile(jderpt_commit_file) == True:
				jderpt_addCommitedFile(jderpt_commit_file)
			else:
				print("File not found: " +  jderpt_commit_file)
		#lst_crpdta
		for crpdta_commit_file in lst_crpdta:
			if os.path.isfile(crpdta_commit_file) == False:
				print("File could not be found: " + crpdta_commit_file + ". Skipping...")
			if os.path.isfile(crpdta_commit_file) == True:
				crpdta_addCommitedFile(crpdta_commit_file)
			else:
				print("File not found: " +  crpdta_commit_file)
		
		#lst_impact
		for impact_commit_file in lst_impact:
			if os.path.isfile(impact_commit_file) == False:
				print("File could not be found: " + impact_commit_file + ". Skipping...")
			if os.path.isfile(impact_commit_file) == True:
				impact_addCommitedFile(impact_commit_file)
			else:
				print("File not found: " +  impact_commit_file)
		#lst_crd
		for crd_commit_file in lst_crd:
			if os.path.isfile(crd_commit_file) == False:
				print("File could not be found: " + crd_commit_file + ". Skipping...")
			if os.path.isfile(crd_commit_file) == True:
				crd_addCommitedFile(crd_commit_file)
			else:
				print("File not found: " +  crd_commit_file)
		#lst_kronos

		for kronos_commit_file in lst_kronos:
			if os.path.isfile(kronos_commit_file) == False:
				print("File could not be found: " + kronos_commit_file + ". Skipping...")
			if os.path.isfile(kronos_commit_file) == True:
				kronos_addCommitedFile(kronos_commit_file)
			else:
				print("File not found: " +  kronos_commit_file)
		#lst_ppm_niku
		for ppm_niku_commit_file in lst_ppm_niku:
			if os.path.isfile(ppm_niku_commit_file) == False:
				print("File could not be found: " + ppm_niku_commit_file + ". Skipping...")
			if os.path.isfile(ppm_niku_commit_file) == True:
				ppm_niku_addCommitedFile(ppm_niku_commit_file)
			else:
				print("File not found: " +  ppm_niku_commit_file)
		#lst_okc
		for okc_commit_file in lst_okc:
			if os.path.isfile(okc_commit_file) == False:
				print("File could not be found: " + okc_commit_file + ". Skipping...")
			if os.path.isfile(okc_commit_file) == True:
				okc_addCommitedFile(okc_commit_file)
			else:
				print("File not found: " +  okc_commit_file)
		#lst_wfm_cloudwfr
		for wfm_cloudwfr_commit_file in lst_wfm_cloudwfr:
			if os.path.isfile(wfm_cloudwfr_commit_file) == False:
				print("File could not be found: " + wfm_cloudwfr_commit_file + ". Skipping...")
			if os.path.isfile(wfm_cloudwfr_commit_file) == True:
				wfm_cloudwfr_addCommitedFile(wfm_cloudwfr_commit_file)
			else:
				print("File not found: " +  wfm_cloudwfr_commit_file)
		#lst_mytime_dbo
		for mytime_dbo_commit_file in lst_mytime_dbo:
			if os.path.isfile(mytime_dbo_commit_file) == False:
				print("File could not be found: " + mytime_dbo_commit_file + ". Skipping...")
			if os.path.isfile(mytime_dbo_commit_file) == True:
				mytime_dbo_addCommitedFile(mytime_dbo_commit_file)
			else:
				print("File not found: " +  mytime_dbo_commit_file)
		#lst_aps_msc
		for aps_msc_commit_file in lst_aps_msc:
			if os.path.isfile(aps_msc_commit_file) == False:
				print("File could not be found: " + aps_msc_commit_file + ". Skipping...")
			if os.path.isfile(aps_msc_commit_file) == True:
				aps_msc_addCommitedFile(aps_msc_commit_file)
			else:
				print("File not found: " +  aps_msc_commit_file)
		#lst_drm
		for drm_commit_file in lst_drm:
			if os.path.isfile(drm_commit_file) == False:
				print("File could not be found: " + drm_commit_file + ". Skipping...")
			if os.path.isfile(drm_commit_file) == True:
				drm_addCommitedFile(drm_commit_file)
			else:
				print("File not found: " +  drm_commit_file)
		if len(lst_centrepiece) > 0 or len(lst_cobra) > 0 or len(lst_ppts) > 0 or len(lst_khw) > 0 or len(lst_golf) > 0 or len(lst_sss) > 0 or len(lst_jderpt) > 0 or len(lst_crpdta) > 0 or len(lst_impact) > 0 or len(lst_crd) > 0 or len(lst_kronos) > 0 or len(lst_ppm_niku) > 0 or len(lst_okc) > 0 or len(lst_wfm_cloudwfr) > 0 or len(lst_mytime_dbo) > 0 or len(lst_aps_msc) > 0 or len(lst_drm) > 0:
			cp_conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			cobra_conn= pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[7], port=sys.argv[3], sslmode="require")
			
			ppts_conn=pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[8], port=sys.argv[3], sslmode="require")
			
			khw_conn=pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			golf_conn=pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			sss_conn=pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			jderpt_conn=pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			crpdta_conn=pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			impact_conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			crd_conn=pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			kronos_conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			ppm_niku_conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			okc_conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			wfm_cloudwfr_conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			mytime_dbo_conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			aps_msc_conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			drm_conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			print("connections established successfully")
			
			cp_cur = cp_conn.cursor()
			cobra_cur = cobra_conn.cursor()
			ppts_cur = ppts_conn.cursor()
			khw_cur = khw_conn.cursor()
			golf_cur = golf_conn.cursor()
			sss_cur = sss_conn.cursor()
			jderpt_cur = jderpt_conn.cursor()
			crpdta_cur = crpdta_conn.cursor()
			impact_cur = impact_conn.cursor()
			crd_cur = crd_conn.cursor()
			kronos_cur = kronos_conn.cursor()
			ppm_niku_cur = ppm_niku_conn.cursor()
			okc_cur = okc_conn.cursor()
			wfm_cloudwfr_cur = wfm_cloudwfr_conn.cursor()
			mytime_dbo_cur = mytime_dbo_conn.cursor()
			aps_msc_cur = aps_msc_conn.cursor()
			drm_cur = drm_conn.cursor()

			build_fsso = sys.argv[4] 
			schema_owner_map = fetch_schema_owners(cp_cur, build_fsso)

			print("Deploying Objects...")

			if len(cp_pre_deployment) > 0:
				pre_deployment_list = sorted(list(cp_pre_deployment))
				writeToDB(pre_deployment_list, cp_cur, None, None, False)
			if len(cp_tables) > 0:
				takeTableBackup(cp_tables, cp_cur)
				writeToDB(cp_tables, cp_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(cp_views) > 0:
				writeToDB(sorted(cp_views), cp_cur, 'table', schema_owner_map, True)
			if len(cp_procedures) > 0:
				writeToDB(cp_procedures, cp_cur, 'procedure', schema_owner_map, True)
			if len(cp_post_deployment) > 0:
				post_deployment_list = sorted(list(cp_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, cp_cur, None, None, False)

			schema_owner_map = fetch_schema_owners(cobra_cur, build_fsso)

			print("Cobra Deploying Objects...")

			if len(cobra_pre_deployment) > 0:
				pre_deployment_list = sorted(list(cobra_pre_deployment))
				writeToDB(pre_deployment_list, cobra_cur, None, None, False)
			if len(cobra_tables) > 0:
				takeTableBackup(cobra_tables, cobra_cur)
				writeToDB(cobra_tables, cobra_cur, 'table', schema_owner_map, True)
				#restoreTableData(cobra_tables, cobra_cur)
			if len(cobra_views) > 0:
				writeToDB(sorted(cobra_views), cobra_cur, 'table', schema_owner_map, True)
			if len(cobra_procedures) > 0:
				writeToDB(cobra_procedures, cobra_cur, 'procedure', schema_owner_map, True)
			if len(cobra_post_deployment) > 0:
				post_deployment_list = sorted(list(cobra_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, cobra_cur, None, None, False)			



			schema_owner_map = fetch_schema_owners(ppts_cur, build_fsso)

			print("ppts Deploying Objects...")

			if len(ppts_pre_deployment) > 0:
				pre_deployment_list = sorted(list(ppts_pre_deployment))
				writeToDB(pre_deployment_list, ppts_cur, None, None, False)
			if len(ppts_tables) > 0:
				takeTableBackup(ppts_tables, ppts_cur)
				writeToDB(ppts_tables, ppts_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(ppts_views) > 0:
				writeToDB(sorted(ppts_views), ppts_cur, 'table', schema_owner_map, True)
			if len(ppts_procedures) > 0:
				writeToDB(ppts_procedures, ppts_cur, 'procedure', schema_owner_map, True)
			if len(ppts_post_deployment) > 0:
				post_deployment_list = sorted(list(ppts_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, ppts_cur, None, None, False)

			schema_owner_map = fetch_schema_owners(khw_cur, build_fsso)

			print("Deploying Objects...")

			if len(khw_pre_deployment) > 0:
				pre_deployment_list = sorted(list(khw_pre_deployment))
				writeToDB(pre_deployment_list, khw_cur, None, None, False)
			if len(khw_tables) > 0:
				takeTableBackup(khw_tables, khw_cur)
				writeToDB(khw_tables, khw_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(khw_views) > 0:
				writeToDB(sorted(khw_views), khw_cur, 'table', schema_owner_map, True)
			if len(khw_procedures) > 0:
				writeToDB(khw_procedures, khw_cur, 'procedure', schema_owner_map, True)
			if len(khw_post_deployment) > 0:
				post_deployment_list = sorted(list(khw_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, khw_cur, None, None, False)


			schema_owner_map = fetch_schema_owners(golf_cur, build_fsso)

			print("Deploying Objects...")

			if len(golf_pre_deployment) > 0:
				pre_deployment_list = sorted(list(golf_pre_deployment))
				writeToDB(pre_deployment_list, golf_cur, None, None, False)
			if len(golf_tables) > 0:
				takeTableBackup(golf_tables, golf_cur)
				writeToDB(golf_tables, golf_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(golf_views) > 0:
				writeToDB(sorted(golf_views), golf_cur, 'table', schema_owner_map, True)
			if len(golf_procedures) > 0:
				writeToDB(golf_procedures, golf_cur, 'procedure', schema_owner_map, True)
			if len(golf_post_deployment) > 0:
				post_deployment_list = sorted(list(golf_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, golf_cur, None, None, False)




			schema_owner_map = fetch_schema_owners(sss_cur, build_fsso)

			print("Deploying Objects...")

			if len(sss_pre_deployment) > 0:
				pre_deployment_list = sorted(list(sss_pre_deployment))
				writeToDB(pre_deployment_list, sss_cur, None, None, False)
			if len(sss_tables) > 0:
				takeTableBackup(sss_tables, sss_cur)
				writeToDB(sss_tables, sss_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(sss_views) > 0:
				writeToDB(sorted(sss_views), sss_cur, 'table', schema_owner_map, True)
			if len(sss_procedures) > 0:
				writeToDB(sss_procedures, sss_cur, 'procedure', schema_owner_map, True)
			if len(sss_post_deployment) > 0:
				post_deployment_list = sorted(list(sss_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, sss_cur, None, None, False)


			schema_owner_map = fetch_schema_owners(jderpt_cur, build_fsso)

			print("Deploying Objects...")

			if len(jderpt_pre_deployment) > 0:
				pre_deployment_list = sorted(list(jderpt_pre_deployment))
				writeToDB(pre_deployment_list, jderpt_cur, None, None, False)
			if len(jderpt_tables) > 0:
				takeTableBackup(jderpt_tables, jderpt_cur)
				writeToDB(jderpt_tables, jderpt_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(jderpt_views) > 0:
				writeToDB(sorted(jderpt_views), jderpt_cur, 'table', schema_owner_map, True)
			if len(jderpt_procedures) > 0:
				writeToDB(jderpt_procedures, jderpt_cur, 'procedure', schema_owner_map, True)
			if len(jderpt_post_deployment) > 0:
				post_deployment_list = sorted(list(jderpt_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, jderpt_cur, None, None, False)

				
			schema_owner_map = fetch_schema_owners(crpdta_cur, build_fsso)

			print("Deploying Objects...")

			if len(crpdta_pre_deployment) > 0:
				pre_deployment_list = sorted(list(crpdta_pre_deployment))
				writeToDB(pre_deployment_list, crpdta_cur, None, None, False)
			if len(crpdta_tables) > 0:
				takeTableBackup(crpdta_tables, crpdta_cur)
				writeToDB(crpdta_tables, crpdta_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(crpdta_views) > 0:
				writeToDB(sorted(crpdta_views), crpdta_cur, 'table', schema_owner_map, True)
			if len(crpdta_procedures) > 0:
				writeToDB(crpdta_procedures, crpdta_cur, 'procedure', schema_owner_map, True)
			if len(crpdta_post_deployment) > 0:
				post_deployment_list = sorted(list(crpdta_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, crpdta_cur, None, None, False)
			


			schema_owner_map = fetch_schema_owners(impact_cur, build_fsso)

			print("Deploying Objects...")

			if len(impact_pre_deployment) > 0:
				pre_deployment_list = sorted(list(impact_pre_deployment))
				writeToDB(pre_deployment_list, impact_cur, None, None, False)
			if len(impact_tables) > 0:
				takeTableBackup(impact_tables, impact_cur)
				writeToDB(impact_tables, impact_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(impact_views) > 0:
				writeToDB(sorted(impact_views), impact_cur, 'table', schema_owner_map, True)
			if len(impact_procedures) > 0:
				writeToDB(impact_procedures, impact_cur, 'procedure', schema_owner_map, True)
			if len(impact_post_deployment) > 0:
				post_deployment_list = sorted(list(impact_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, impact_cur, None, None, False)

			schema_owner_map = fetch_schema_owners(crd_cur, build_fsso)

			print("Deploying Objects...")

			if len(crd_pre_deployment) > 0:
				pre_deployment_list = sorted(list(crd_pre_deployment))
				writeToDB(pre_deployment_list, crd_cur, None, None, False)
			if len(crd_tables) > 0:
				takeTableBackup(crd_tables, crd_cur)
				writeToDB(crd_tables, crd_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(crd_views) > 0:
				writeToDB(sorted(crd_views), crd_cur, 'table', schema_owner_map, True)
			if len(crd_procedures) > 0:
				writeToDB(crd_procedures, crd_cur, 'procedure', schema_owner_map, True)
			if len(crd_post_deployment) > 0:
				post_deployment_list = sorted(list(crd_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, crd_cur, None, None, False)

			schema_owner_map = fetch_schema_owners(ppm_niku_cur, build_fsso)

			print("Deploying Objects...")

			if len(ppm_niku_pre_deployment) > 0:
				pre_deployment_list = sorted(list(ppm_niku_pre_deployment))
				writeToDB(pre_deployment_list, ppm_niku_cur, None, None, False)
			if len(ppm_niku_tables) > 0:
				takeTableBackup(ppm_niku_tables, ppm_niku_cur)
				writeToDB(ppm_niku_tables, ppm_niku_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(ppm_niku_views) > 0:
				writeToDB(sorted(ppm_niku_views), ppm_niku_cur, 'table', schema_owner_map, True)
			if len(ppm_niku_procedures) > 0:
				writeToDB(ppm_niku_procedures, ppm_niku_cur, 'procedure', schema_owner_map, True)
			if len(ppm_niku_post_deployment) > 0:
				post_deployment_list = sorted(list(ppm_niku_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, ppm_niku_cur, None, None, False)

			schema_owner_map = fetch_schema_owners(okc_cur, build_fsso)

			print("Deploying Objects...")

			if len(okc_pre_deployment) > 0:
				pre_deployment_list = sorted(list(okc_pre_deployment))
				writeToDB(pre_deployment_list, okc_cur, None, None, False)
			if len(okc_tables) > 0:
				takeTableBackup(okc_tables, okc_cur)
				writeToDB(okc_tables, okc_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(okc_views) > 0:
				writeToDB(sorted(okc_views), okc_cur, 'table', schema_owner_map, True)
			if len(okc_procedures) > 0:
				writeToDB(okc_procedures, okc_cur, 'procedure', schema_owner_map, True)
			if len(okc_post_deployment) > 0:
				post_deployment_list = sorted(list(okc_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, okc_cur, None, None, False)

			schema_owner_map = fetch_schema_owners(wfm_cloudwfr_cur, build_fsso)

			print("Deploying Objects...")

			if len(wfm_cloudwfr_pre_deployment) > 0:
				pre_deployment_list = sorted(list(wfm_cloudwfr_pre_deployment))
				writeToDB(pre_deployment_list, wfm_cloudwfr_cur, None, None, False)
			if len(wfm_cloudwfr_tables) > 0:
				takeTableBackup(wfm_cloudwfr_tables, wfm_cloudwfr_cur)
				writeToDB(wfm_cloudwfr_tables, wfm_cloudwfr_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(wfm_cloudwfr_views) > 0:
				writeToDB(sorted(wfm_cloudwfr_views), wfm_cloudwfr_cur, 'table', schema_owner_map, True)
			if len(wfm_cloudwfr_procedures) > 0:
				writeToDB(wfm_cloudwfr_procedures, wfm_cloudwfr_cur, 'procedure', schema_owner_map, True)
			if len(wfm_cloudwfr_post_deployment) > 0:
				post_deployment_list = sorted(list(wfm_cloudwfr_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, wfm_cloudwfr_cur, None, None, False)

			schema_owner_map = fetch_schema_owners(mytime_dbo_cur, build_fsso)

			print("Deploying Objects...")

			if len(mytime_dbo_pre_deployment) > 0:
				pre_deployment_list = sorted(list(mytime_dbo_pre_deployment))
				writeToDB(pre_deployment_list, mytime_dbo_cur, None, None, False)
			if len(mytime_dbo_tables) > 0:
				takeTableBackup(mytime_dbo_tables, mytime_dbo_cur)
				writeToDB(mytime_dbo_tables, mytime_dbo_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(mytime_dbo_views) > 0:
				writeToDB(sorted(mytime_dbo_views), mytime_dbo_cur, 'table', schema_owner_map, True)
			if len(mytime_dbo_procedures) > 0:
				writeToDB(mytime_dbo_procedures, mytime_dbo_cur, 'procedure', schema_owner_map, True)
			if len(mytime_dbo_post_deployment) > 0:
				post_deployment_list = sorted(list(mytime_dbo_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, mytime_dbo_cur, None, None, False)

			schema_owner_map = fetch_schema_owners(aps_msc_cur, build_fsso)

			print("Deploying Objects...")

			if len(aps_msc_pre_deployment) > 0:
				pre_deployment_list = sorted(list(aps_msc_pre_deployment))
				writeToDB(pre_deployment_list, aps_msc_cur, None, None, False)
			if len(aps_msc_tables) > 0:
				takeTableBackup(aps_msc_tables, aps_msc_cur)
				writeToDB(aps_msc_tables, aps_msc_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(aps_msc_views) > 0:
				writeToDB(sorted(aps_msc_views), aps_msc_cur, 'table', schema_owner_map, True)
			if len(aps_msc_procedures) > 0:
				writeToDB(aps_msc_procedures, aps_msc_cur, 'procedure', schema_owner_map, True)
			if len(aps_msc_post_deployment) > 0:
				post_deployment_list = sorted(list(aps_msc_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, aps_msc_cur, None, None, False)

			schema_owner_map = fetch_schema_owners(drm_cur, build_fsso)

			print("Deploying Objects...")

			if len(drm_pre_deployment) > 0:
				pre_deployment_list = sorted(list(drm_pre_deployment))
				writeToDB(pre_deployment_list, drm_cur, None, None, False)
			if len(drm_tables) > 0:
				takeTableBackup(drm_tables, drm_cur)
				writeToDB(drm_tables, drm_cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(drm_views) > 0:
				writeToDB(sorted(drm_views), drm_cur, 'table', schema_owner_map, True)
			if len(drm_procedures) > 0:
				writeToDB(drm_procedures, drm_cur, 'procedure', schema_owner_map, True)
			if len(drm_post_deployment) > 0:
				post_deployment_list = sorted(list(drm_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, drm_cur, None, None, False)

			cp_conn.commit()
			cobra_conn.commit()
			ppts_conn.commit()
			khw_conn.commit()
			golf_conn.commit()
			sss_conn.commit()
			jderpt_conn.commit()
			crpdta_conn.commit()
			impact_conn.commit()
			crd_conn.commit()
			kronos_conn.commit()
			ppm_niku_conn.commit()
			okc_conn.commit()
			wfm_cloudwfr_conn.commit()
			mytime_dbo_conn.commit()
			aps_msc_conn.commit()
			drm_conn.commit()
			print('-----------------------------------------------------------------------')
			print('---------------------- Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(cp_tables) > 0:
				print("Total number of tables : "+str(len(cp_tables)))							
			if len(cp_views) > 0:
				print("Total number of views : "+str(len(cp_views)))
			if len(cp_procedures) > 0:
				print("Total number of procedures : "+str(len(cp_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(cobra_tables) > 0:
				print("Total number of tables : "+str(len(cobra_tables)))							
			if len(cobra_views) > 0:
				print("Total number of views : "+str(len(cobra_views)))
			if len(cobra_procedures) > 0:
				print("Total number of procedures : "+str(len(cobra_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(ppts_tables) > 0:
				print("Total number of tables : "+str(len(ppts_tables)))							
			if len(ppts_views) > 0:
				print("Total number of views : "+str(len(ppts_views)))
			if len(ppts_procedures) > 0:
				print("Total number of procedures : "+str(len(ppts_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(khw_tables) > 0:
				print("Total number of tables : "+str(len(khw_tables)))							
			if len(khw_views) > 0:
				print("Total number of views : "+str(len(khw_views)))
			if len(khw_procedures) > 0:
				print("Total number of procedures : "+str(len(khw_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(golf_tables) > 0:
				print("Total number of tables : "+str(len(golf_tables)))							
			if len(golf_views) > 0:
				print("Total number of views : "+str(len(golf_views)))
			if len(golf_procedures) > 0:
				print("Total number of procedures : "+str(len(golf_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(sss_tables) > 0:
				print("Total number of tables : "+str(len(sss_tables)))							
			if len(sss_views) > 0:
				print("Total number of views : "+str(len(sss_views)))
			if len(sss_procedures) > 0:
				print("Total number of procedures : "+str(len(sss_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(jderpt_tables) > 0:
				print("Total number of tables : "+str(len(jderpt_tables)))							
			if len(jderpt_views) > 0:
				print("Total number of views : "+str(len(jderpt_views)))
			if len(jderpt_procedures) > 0:
				print("Total number of procedures : "+str(len(jderpt_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(crpdta_tables) > 0:
				print("Total number of tables : "+str(len(crpdta_tables)))							
			if len(crpdta_views) > 0:
				print("Total number of views : "+str(len(crpdta_views)))
			if len(crpdta_procedures) > 0:
				print("Total number of procedures : "+str(len(crpdta_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(impact_tables) > 0:
				print("Total number of tables : "+str(len(impact_tables)))							
			if len(impact_views) > 0:
				print("Total number of views : "+str(len(impact_views)))
			if len(impact_procedures) > 0:
				print("Total number of procedures : "+str(len(impact_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- crd Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(crd_tables) > 0:
				print("Total number of tables : "+str(len(crd_tables)))							
			if len(crd_views) > 0:
				print("Total number of views : "+str(len(crd_views)))
			if len(crd_procedures) > 0:
				print("Total number of procedures : "+str(len(crd_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- ppm_niku Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(ppm_niku_tables) > 0:
				print("Total number of tables : "+str(len(ppm_niku_tables)))							
			if len(ppm_niku_views) > 0:
				print("Total number of views : "+str(len(ppm_niku_views)))
			if len(ppm_niku_procedures) > 0:
				print("Total number of procedures : "+str(len(ppm_niku_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- okc Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(okc_tables) > 0:
				print("Total number of tables : "+str(len(okc_tables)))							
			if len(okc_views) > 0:
				print("Total number of views : "+str(len(okc_views)))
			if len(okc_procedures) > 0:
				print("Total number of procedures : "+str(len(okc_procedures)))

			print('-----------------------------------------------------------------------')
			print('-----------------------------------------------------------------------')
			print('---------------------- wfm_cloudwfr Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(wfm_cloudwfr_tables) > 0:
				print("Total number of tables : "+str(len(wfm_cloudwfr_tables)))							
			if len(wfm_cloudwfr_views) > 0:
				print("Total number of views : "+str(len(wfm_cloudwfr_views)))
			if len(wfm_cloudwfr_procedures) > 0:
				print("Total number of procedures : "+str(len(wfm_cloudwfr_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- mytime_dbo Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(mytime_dbo_tables) > 0:
				print("Total number of tables : "+str(len(mytime_dbo_tables)))							
			if len(mytime_dbo_views) > 0:
				print("Total number of views : "+str(len(mytime_dbo_views)))
			if len(mytime_dbo_procedures) > 0:
				print("Total number of procedures : "+str(len(mytime_dbo_procedures)))
			print('-----------------------------------------------------------------------')
			print('-----------------------------------------------------------------------')
			print('---------------------- aps_msc Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(aps_msc_tables) > 0:
				print("Total number of tables : "+str(len(aps_msc_tables)))							
			if len(aps_msc_views) > 0:
				print("Total number of views : "+str(len(aps_msc_views)))
			if len(aps_msc_procedures) > 0:
				print("Total number of procedures : "+str(len(aps_msc_procedures)))
			print('-----------------------------------------------------------------------')

			print('-----------------------------------------------------------------------')
			print('---------------------- drm Deployment Summary Report ----------------------')
			print('-----------------------------------------------------------------------')
			if len(drm_tables) > 0:
				print("Total number of tables : "+str(len(drm_tables)))							
			if len(drm_views) > 0:
				print("Total number of views : "+str(len(drm_views)))
			if len(drm_procedures) > 0:
				print("Total number of procedures : "+str(len(drm_procedures)))
			print('-----------------------------------------------------------------------')

		else:
			print("No valid files found to deploy in <files_to_deploy>: " + commit_file)

	except Exception as e:
		print("DEPLOYER ERROR: " + getattr(e, 'strerror', str(e)))
		if cp_cur is not None:
			cp_conn.rollback()
			sys.exit(200)
		if cobra_cur is not None:
			cobra_conn.rollback()
			sys.exit(200)
		if ppts_cur is not None:
			ppts_conn.rollback()
			sys.exit(200)
		if khw_cur is not None:
			khw_conn.rollback()
			sys.exit(200)
		if golf_cur is not None:
			golf_conn.rollback()
			sys.exit(200)

		if sss_cur is not None:
			sss_conn.rollback()
			sys.exit(200)

		if jderpt_cur is not None:
			jderpt_conn.rollback()
			sys.exit(200)	

		if crpdta_cur is not None:
			crpdta_conn.rollback()
			sys.exit(200)

		if impact_cur is not None:
			impact_conn.rollback()
			sys.exit(200)
		if crd_cur is not None:
			crd_conn.rollback()
			sys.exit(200)
		if kronos_cur is not None:
			kronos_conn.rollback()
			sys.exit(200)

		if ppm_niku_cur is not None:
			ppm_niku_conn.rollback()
			sys.exit(200)

		if okc_cur is not None:
			okc_conn.rollback()
			sys.exit(200)

		if wfm_cloudwfr_cur is not None:
			wfm_cloudwfr_conn.rollback()
			sys.exit(200)

		if mytime_dbo_cur is not None:
			mytime_dbo_conn.rollback()
			sys.exit(200)	
		if aps_msc_cur is not None:
			aps_msc_conn.rollback()
			sys.exit(200)
											
		if drm_cur is not None:
			drm_conn.rollback()
			sys.exit(200)					
	finally:
		if cp_cur is not None:
			cp_cur.close()
		if cp_conn is not None:
			cp_conn.close()
		if cobra_cur is not None:
			cobra_cur.close()
		if cobra_conn is not None:
			cobra_conn.close()
		if ppts_cur is not None:
			ppts_cur.close()
		if ppts_conn is not None:
			ppts_conn.close()

		if khw_cur is not None:
			khw_cur.close()
		if khw_conn is not None:
			khw_conn.close()

		if golf_cur is not None:
			golf_cur.close()
		if golf_conn is not None:
			golf_conn.close()

		if sss_cur is not None:
			sss_cur.close()
		if sss_conn is not None:
			sss_conn.close()
		if jderpt_cur is not None:
			jderpt_cur.close()
		if jderpt_conn is not None:
			jderpt_conn.close()
		if crpdta_cur is not None:
			crpdta_cur.close()
		if crpdta_conn is not None:
			crpdta_conn.close()

		if impact_cur is not None:
			impact_cur.close()
		if impact_conn is not None:
			impact_conn.close()
		if crd_cur is not None:
			crd_cur.close()
		if crd_conn is not None:
			crd_conn.close()
		if kronos_cur is not None:
			kronos_cur.close()
		if kronos_conn is not None:
			kronos_conn.close()

		if ppm_niku_cur is not None:
			ppm_niku_cur.close()
		if ppm_niku_conn is not None:
			ppm_niku_conn.close()

		if okc_cur is not None:
			okc_cur.close()
		if okc_conn is not None:
			okc_conn.close()

		if wfm_cloudwfr_cur is not None:
			wfm_cloudwfr_cur.close()
		if wfm_cloudwfr_conn is not None:
			wfm_cloudwfr_conn.close()

		if mytime_dbo_cur is not None:
			mytime_dbo_cur.close()
		if mytime_dbo_conn is not None:
			mytime_dbo_conn.close()

		if aps_msc_cur is not None:
			aps_msc_cur.close()
		if aps_msc_conn is not None:
			aps_msc_conn.close()
		if drm_cur is not None:
			drm_cur.close()
		if drm_conn is not None:
			drm_conn.close()									
	print("Done")
	sys.exit(0)

	
if __name__ == '__main__':
	main()