import sys
import psycopg2 as pg
import os
from io import open
file_path = os.path.dirname(os.path.abspath(__file__))+os.sep

#a=['./SQL/centrepiece/schema/tables/a.ddl', './SQL/centrepiece/schema/pre_deployment/a.sql', './SQL/centrepiece/schema/post_deployment/a.sql', './SQL/centrepiece/schema/views/a.sql', './SQL/centrepiece/schema/procedures/a.sql', './SQL/cobra/schema/pre_deployment/a.sql','./SQL/cobra/schema/tables/a.ddl','./SQL/cobra/schema/post_deployment/a.sql','./SQL/cobra/schema/views/a.sql','./SQL/cobra/schema/procedures/a.sql']
commit_file = file_path+'git_diff_files_12345'
git_commit_list = []	
with open(commit_file,'r') as f:	
	for line in f.read().splitlines():
		git_commit_list.append('.' + os.sep + line)			  								
print(git_commit_list)
cp_pre_deplotment = set()
cp_post_deployment = set()
cp_tables = set()
cp_views = set()
cp_procedures = set()

cobra_pre_deplotment = set()
cobra_post_deployment = set()
cobra_tables = set()
cobra_views = set()
cobra_procedures = set()

ppts_pre_deplotment = set()
ppts_post_deployment = set()
ppts_tables = set()
ppts_views = set()
ppts_procedures = set()

khw_pre_deplotment = set()
khw_post_deployment = set()
khw_tables = set()
khw_views = set()
khw_procedures = set()

golf_pre_deplotment = set()
golf_post_deployment = set()
golf_tables = set()
golf_views = set()
golf_procedures = set()


sss_pre_deplotment = set()
sss_post_deployment = set()
sss_tables = set()
sss_views = set()
sss_procedures = set()

jderpt_pre_deplotment = set()
jderpt_post_deployment = set()
jderpt_tables = set()
jderpt_views = set()
jderpt_procedures = set()

crpdta_pre_deplotment = set()
crpdta_post_deployment = set()
crpdta_tables = set()
crpdta_views = set()
crpdta_procedures = set()

impact_pre_deplotment = set()
impact_post_deployment = set()
impact_tables = set()
impact_views = set()
impact_procedures = set()

crd_pre_deplotment = set()
crd_post_deployment = set()
crd_tables = set()
crd_views = set()
crd_procedures = set()


kronos_pre_deplotment = set()
kronos_post_deployment = set()
kronos_tables = set()
kronos_views = set()
kronos_procedures = set()

ppm_niku_pre_deplotment = set()
ppm_niku_post_deployment = set()
ppm_niku_tables = set()
ppm_niku_views = set()
ppm_niku_procedures = set()

okc_pre_deplotment = set()
okc_post_deployment = set()
okc_tables = set()
okc_views = set()
okc_procedures = set()

wfm_cloudwfr_pre_deplotment = set()
wfm_cloudwfr_post_deployment = set()
wfm_cloudwfr_tables = set()
wfm_cloudwfr_views = set()
wfm_cloudwfr_procedures = set()

mytime_dbo_pre_deplotment = set()
mytime_dbo_post_deployment = set()
mytime_dbo_tables = set()
mytime_dbo_views = set()
mytime_dbo_procedures = set()

aps_msc_pre_deplotment = set()
aps_msc_post_deployment = set()
aps_msc_tables = set()
aps_msc_views = set()
aps_msc_procedures = set()

drm_pre_deplotment = set()
drm_post_deployment = set()
drm_tables = set()
drm_views = set()
drm_procedures = set()







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





for source in git_commit_list:
    if os.path.isfile(source) == False:
        print("File could not be found: " + source + ". Skipping...")
    if os.path.isfile(source) == True:
        if source.endswith('.sql'):
            if source.find('/centrepiece/') > 0:
                lst_centrepiece.append(source)
            elif source.find('/cobra/') > 0:
                lst_cobra.append(source)
            elif source.find('/ppts/') > 0:
                lst_ppts.append(source)
            elif source.find('/khw/') > 0:
                lst_khw.append(source)
            elif source.find('/golf/') > 0:
                lst_golf.append(source)
            elif source.find('/sss/') > 0:
                lst_sss.append(source)
            elif source.find('/jderpt/') > 0:
                lst_jderpt.append(source)
            elif source.find('/crpdta/') > 0:
                lst_crpdta.append(source)
            elif source.find('/impact/') > 0:
                lst_impact.append(source)
            elif source.find('/crd/') > 0:
                lst_crd.append(source)
            elif source.find('/kronos/') > 0:
                lst_kronos.append(source)
            elif source.find('/ppm_niku/') > 0:
                lst_ppm_niku.append(source)
            elif source.find('/okc/') > 0:
                lst_okc.append(source)
            elif source.find('/wfm_cloudwfr/') > 0:
                lst_wfm_cloudwfr.append(source)
            elif source.find('/mytime_dbo/') > 0:
                lst_mytime_dbo.append(source)
            elif source.find('/aps_msc/') > 0:
                lst_aps_msc.append(source)
            elif source.find('/drm/') > 0:
                lst_drm.append(source)                

        if source.endswith('.ddl'):
            if source.find('/centrepiece/') > 0:
                lst_centrepiece.append(source)
            elif source.find('/cobra/') > 0:
                lst_cobra.append(source)
            elif source.find('/ppts/') > 0:
                lst_ppts.append(source)
            elif source.find('/khw/') > 0:
                lst_khw.append(source)
            elif source.find('/golf/') > 0:
                lst_golf.append(source)
            elif source.find('/sss/') > 0:
                lst_sss.append(source)
            elif source.find('/jderpt/') > 0:
                lst_jderpt.append(source)
            elif source.find('/crpdta/') > 0:
                lst_crpdta.append(source)
            elif source.find('/impact/') > 0:
                lst_impact.append(source)
            elif source.find('/crd/') > 0:
                lst_crd.append(source)
            elif source.find('/kronos/') > 0:
                lst_kronos.append(source)
            elif source.find('/ppm_niku/') > 0:
                lst_ppm_niku.append(source)
            elif source.find('/okc/') > 0:
                lst_okc.append(source)
            elif source.find('/wfm_cloudwfr/') > 0:
                lst_wfm_cloudwfr.append(source)
            elif source.find('/mytime_dbo/') > 0:
                lst_mytime_dbo.append(source)
            elif source.find('/aps_msc/') > 0:
                lst_aps_msc.append(source)
            elif source.find('/drm/') > 0:
                lst_drm.append(source)                




print('centrepiece list of files : ', lst_centrepiece)
print('cobra list of files : ',lst_cobra)
print('ppts list of files : ',lst_ppts )
print('khw list of files : ',lst_khw)
print('golf list of files : ',lst_golf )
print('sss list of files : ',lst_sss)
print('jderpt list of files : ',lst_jderpt)
print('crpdta list of files : ', lst_crpdta)
print('impact list of files : ',lst_impact)
print('crd list of files : ',lst_crd)
print('kronos list of files : ',lst_kronos )
print('ppm_niku list of files : ',lst_ppm_niku)
print('okc list of files : ',lst_okc)
print('wfm_cloudwfrlist of files : ',lst_wfm_cloudwfr)
print('mytime_dbolist of files : ',lst_mytime_dbo)
print('aps_msc list of files : ',lst_aps_msc)
print('drm list of files : ',lst_drm)



for filename in lst_centrepiece:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                cp_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                cp_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                cp_procedures.add(filename)
            elif filename.find('/views/') > 0:
                cp_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                cp_tables.add(filename)

# Cobra Objects

for filename in lst_cobra:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                cobra_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                cobra_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                cobra_procedures.add(filename)
            elif filename.find('/views/') > 0:
                cobra_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                cobra_tables.add(filename)

# ppts objects
for filename in lst_ppts:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                ppts_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                ppts_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                ppts_procedures.add(filename)
            elif filename.find('/views/') > 0:
                ppts_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                ppts_tables.add(filename)                
# khw Objects
for filename in lst_khw:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                khw_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                khw_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                khw_procedures.add(filename)
            elif filename.find('/views/') > 0:
                khw_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                khw_tables.add(filename)
# golf Objects
for filename in lst_golf:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                golf_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                golf_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                golf_procedures.add(filename)
            elif filename.find('/views/') > 0:
                golf_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                golf_tables.add(filename)

# sss Objects
for filename in lst_sss:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                sss_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                sss_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                sss_procedures.add(filename)
            elif filename.find('/views/') > 0:
                sss_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                sss_tables.add(filename)

# jderpt Objects

for filename in lst_jderpt:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                jderpt_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                jderpt_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                jderpt_procedures.add(filename)
            elif filename.find('/views/') > 0:
                jderpt_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                jderpt_tables.add(filename)

# crpdta Objects

for filename in lst_crpdta:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                crpdta_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                crpdta_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                crpdta_procedures.add(filename)
            elif filename.find('/views/') > 0:
                crpdta_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                crpdta_tables.add(filename)
# impact Objects
for filename in lst_impact:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                impact_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                impact_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                impact_procedures.add(filename)
            elif filename.find('/views/') > 0:
                impact_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                impact_tables.add(filename)
# crd objects
for filename in lst_crd:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                crd_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                crd_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                crd_procedures.add(filename)
            elif filename.find('/views/') > 0:
                crd_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                crd_tables.add(filename)
# kronos Objects
for filename in lst_kronos:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                kronos_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                kronos_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                kronos_procedures.add(filename)
            elif filename.find('/views/') > 0:
                kronos_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                kronos_tables.add(filename)

# ppm_niku Objects
for filename in lst_ppm_niku:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                ppm_niku_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                ppm_niku_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                ppm_niku_procedures.add(filename)
            elif filename.find('/views/') > 0:
                ppm_niku_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                ppm_niku_tables.add(filename)

# okc Objects
for filename in lst_okc:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                okc_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                okc_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                okc_procedures.add(filename)
            elif filename.find('/views/') > 0:
                okc_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                okc_tables.add(filename)
# wfm_cloudwfr Objects

for filename in lst_wfm_cloudwfr:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                wfm_cloudwfr_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                wfm_cloudwfr_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                wfm_cloudwfr_procedures.add(filename)
            elif filename.find('/views/') > 0:
                wfm_cloudwfr_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                wfm_cloudwfr_tables.add(filename)
# mytime_dbo Objects
for filename in lst_mytime_dbo:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                mytime_dbo_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                mytime_dbo_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                mytime_dbo_procedures.add(filename)
            elif filename.find('/views/') > 0:
                mytime_dbo_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                mytime_dbo_tables.add(filename)

# aps_msc objects

for filename in lst_aps_msc:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                aps_msc_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                aps_msc_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                aps_msc_procedures.add(filename)
            elif filename.find('/views/') > 0:
                aps_msc_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                aps_msc_tables.add(filename)

# drm Objects

for filename in lst_drm:
    if os.path.isfile(filename) == False:
        print("File could not be found: " + filename + ". Skipping...")
    if os.path.isfile(filename) == True:
        if filename.endswith('.sql'):
            if filename.find('/pre_deployment/') > 0:
                drm_pre_deplotment.add(filename)
            elif filename.find('/post_deployment/') > 0:
                drm_post_deployment.add(filename)
            elif filename.find('/procedures/') > 0:
                drm_procedures.add(filename)
            elif filename.find('/views/') > 0:
                drm_views.add(filename)                                
        if filename.endswith('.ddl'):
            if filename.find('/tables/') > 0:
                drm_tables.add(filename)

print('cp_pre_deplotment : ',cp_pre_deplotment)
print()
print('cp_post_deployment : ',cp_post_deployment)
print()
print('cp_tables : ',cp_tables)
print()
print('cp_views : ',cp_views)
print()
print('cp_procedures : ',cp_procedures)
print()
print('cobra_pre_deplotment : ',cobra_pre_deplotment)
print()
print('cobra_post_deployment : ',cobra_post_deployment)
print()
print('cobra_tables : ',cobra_tables)
print()
print('cobra_views : ',cobra_views)
print()
print('cobra_procedures : ',cobra_procedures)
print()

print('ppts_pre_deplotment : ',ppts_pre_deplotment)
print()
print('ppts_post_deployment : ',ppts_post_deployment)
print()
print('ppts_tables : ',ppts_tables)
print()
print('ppts_views : ',ppts_views)
print()
print('ppts_procedures : ',ppts_procedures)
print()


print('khw_pre_deplotment : ',khw_pre_deplotment)
print()
print('khw_post_deployment : ',khw_post_deployment)
print()
print('khw_tables : ',khw_tables)
print()
print('khw_views : ',khw_views)
print()
print('khw_procedures : ',khw_procedures)
print()

print('golf_pre_deplotment : ',golf_pre_deplotment)
print()
print('golf_post_deployment : ',golf_post_deployment)
print()
print('golf_tables : ',golf_tables)
print()
print('golf_views : ',golf_views)
print()
print('golf_procedures : ',golf_procedures)
print()

print('sss_pre_deplotment : ',sss_pre_deplotment)
print()
print('sss_post_deployment : ',sss_post_deployment)
print()
print('sss_tables : ',sss_tables)
print()
print('sss_views : ',sss_views)
print()
print('sss_procedures : ',sss_procedures)
print()

print('jderpt_pre_deplotment : ',jderpt_pre_deplotment)
print()
print('jderpt_post_deployment : ',jderpt_post_deployment)
print()
print('jderpt_tables : ',jderpt_tables)
print()
print('jderpt_views : ',jderpt_views)
print()
print('jderpt_procedures : ',jderpt_procedures)
print()

print('crpdta_pre_deplotment : ',crpdta_pre_deplotment)
print()
print('crpdta_post_deployment : ',crpdta_post_deployment)
print()
print('crpdta_tables : ',crpdta_tables)
print()
print('crpdta_views : ',crpdta_views)
print()
print('crpdta_procedures : ',crpdta_procedures)
print()

print('impact_pre_deplotment : ',impact_pre_deplotment)
print()
print('impact_post_deployment : ',impact_post_deployment)
print()
print('impact_tables : ',impact_tables)
print()
print('impact_views : ',impact_views)
print()
print('impact_procedures : ',impact_procedures)
print()

print('crd_pre_deplotment : ',crd_pre_deplotment)
print()
print('crd_post_deployment : ',crd_post_deployment)
print()
print('crd_tables : ',crd_tables)
print()
print('crd_views : ',crd_views)
print()
print('crd_procedures : ',crd_procedures)
print()

print('kronos_pre_deplotment : ',kronos_pre_deplotment)
print()
print('kronos_post_deployment : ',kronos_post_deployment)
print()
print('kronos_tables : ',kronos_tables)
print()
print('kronos_views : ',kronos_views)
print()
print('kronos_procedures : ',kronos_procedures)
print()

print('ppm_niku_pre_deplotment : ',ppm_niku_pre_deplotment)
print()
print('ppm_niku_post_deployment : ',ppm_niku_post_deployment)
print()
print('ppm_niku_tables : ',ppm_niku_tables)
print()
print('ppm_niku_views : ',ppm_niku_views)
print()
print('ppm_niku_procedures : ',ppm_niku_procedures)
print()

print('okc_pre_deplotment : ',okc_pre_deplotment)
print()
print('okc_post_deployment : ',okc_post_deployment)
print()
print('okc_tables : ',okc_tables)
print()
print('okc_views : ',okc_views)
print()
print('okc_procedures : ',okc_procedures)
print()

print('wfm_cloudwfr_pre_deplotment : ',wfm_cloudwfr_pre_deplotment)
print()
print('wfm_cloudwfr_post_deployment : ',wfm_cloudwfr_post_deployment)
print()
print('wfm_cloudwfr_tables : ',wfm_cloudwfr_tables)
print()
print('wfm_cloudwfr_views : ',wfm_cloudwfr_views)
print()
print('wfm_cloudwfr_procedures : ',wfm_cloudwfr_procedures)
print()

print('mytime_dbo_pre_deplotment : ',mytime_dbo_pre_deplotment)
print()
print('mytime_dbo_post_deployment : ',mytime_dbo_post_deployment)
print()
print('mytime_dbo_tables : ',mytime_dbo_tables)
print()
print('mytime_dbo_views : ',mytime_dbo_views)
print()
print('mytime_dbo_procedures : ',mytime_dbo_procedures)
print()

print('drm_pre_deplotment : ',drm_pre_deplotment)
print()
print('drm_post_deployment : ',drm_post_deployment)
print()
print('drm_tables : ',drm_tables)
print()
print('drm_views : ',drm_views)
print()
print('drm_procedures : ',drm_procedures)
print()












