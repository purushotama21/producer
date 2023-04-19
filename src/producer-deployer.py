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

	conn = None
	cur = None
	try:

		# Argument validation: make sure the correct number of arguments was supplied
		if len(sys.argv) != 7 or len(sys.argv[6]) < 1:
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
		

		for cp_commit_file in lst_centrepiece:
			if os.path.isfile(cp_commit_file) == False:
				print("File could not be found: " + cp_commit_file + ". Skipping...")
			if os.path.isfile(cp_commit_file) == True:
				addCommitedFile(cp_commit_file)
			else:
				print("File not found: " +  cp_commit_file)


		if len(lst_centrepiece) > 0 or len(lst_cobra) > 0 or len(lst_ppts) > 0 or len(lst_khw) > 0 or len(lst_golf) > 0 or len(lst_sss) > 0 or len(lst_jderpt) > 0 or len(lst_crpdta) > 0 or len(lst_impact) > 0 or len(lst_crd) > 0 or len(lst_kronos) > 0 or len(lst_ppm_niku) > 0 or len(lst_okc) > 0 or len(lst_wfm_cloudwfr) > 0 or len(lst_mytime_dbo) > 0 or len(lst_aps_msc) > 0 or len(lst_drm) > 0:
			cp_conn = pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			cobra_conn= pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
			ppts_conn=pg.connect(host=sys.argv[1], user=sys.argv[4], password=sys.argv[5], database=sys.argv[2], port=sys.argv[3], sslmode="require")
			
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

			schema_owner_map = fetch_schema_owners(cur, build_fsso)

			print("Deploying Objects...")

			if len(cp_pre_deployment) > 0:
				pre_deployment_list = sorted(list(cp_pre_deployment))
				writeToDB(pre_deployment_list, cp_cur, None, None, False)
			if len(cp_tables) > 0:
				#takeTableBackup(tables, cur)
				writeToDB(cp_tables, cur, 'table', schema_owner_map, True)
				#restoreTableData(tables, cur)
			if len(cp_views) > 0:
				writeToDB(sorted(cp_views), cp_cur, 'table', schema_owner_map, True)
			if len(cp_procedures) > 0:
				writeToDB(cp_procedures, cp_cur, 'procedure', schema_owner_map, True)
			if len(cp_post_deployment) > 0:
				post_deployment_list = sorted(list(cp_post_deployment)) #Order by filename
				writeToDB(post_deployment_list, cp_cur, None, None, False)


			conn.commit()
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
	print("Done")
	sys.exit(0)

	
if __name__ == '__main__':
	main()