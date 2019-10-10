'''
    This script is executed as part of the build pipeline to
        - Identify only those models which are persistent in nature.
        - Dedup multiple occurence of same models.

    Model files are identified only if they are present in the sub-directories under 'models'. Deployable is based of
    materialization, currently only 'persistent_tables' are supported.
    The script is to be invoked after the 'IdentifyGitBuildCommitItems.py', as this uses the resulting file 'ListOfCommitItems.txt'.
       The filtered list of models to be deployed are stored in the file 'DeployableModels.txt'
'''
import logging
from types import SimpleNamespace
import os

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

'''
    - Loads the list of committed files
    - Dedup
    - return the list
'''
def getListOfCommitedModelScripts(filepath):
    logger.info("Reading file : %s ...",filepath)
    lineList = list()
    with open(filepath) as f:
        for line in f:
            ln = line.rstrip('\n').split(',')[1]
            if '.sql' in ln:
                lineList.append(ln)
    listof_committed_files = set(lineList)
    return listof_committed_files

'''
    Filter only 
        - model based script
        - models that are persistable (ex: persistent_table) materialization.
'''
def filterDeployableModels(listof_committed_files):
    logger.info('Iterating and filtering deployable models ...')
    deployable_models_list = []

    for mdl in listof_committed_files:
        if 'models/' in mdl:
            with open(mdl, 'r') as fl:
                fl_content = fl.read()
                if ('persistent_table' in fl_content):
                    deployable_models_list.append(mdl)

    return deployable_models_list;

def savefiles_toexecute(filepath ,filtered_deployable_list):
    logger.info("Saving the identified deployable model scripts at : %s ...",filepath)
    with open(filepath, 'w') as out:
        for sql_script in filtered_deployable_list:
            out.write("{}\n".format(sql_script) )


if __name__ == "__main__":
    artifact_staging_dir = os.environ.get('SYSTEM_ARTIFACTSDIRECTORY')
    listof_committed_files = getListOfCommitedModelScripts(artifact_staging_dir + '/ListOfCommitItems.txt')
    filtered_deployable_list = filterDeployableModels(listof_committed_files)
    savefiles_toexecute(artifact_staging_dir + '/DeployableModels.txt',filtered_deployable_list)

    logger.info(filtered_deployable_list)
    logger.info('Finished !!!')
