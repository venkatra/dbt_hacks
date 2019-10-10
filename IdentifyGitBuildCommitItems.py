'''
    This is an utility script that is invoked as part of the build pipeline. It is used for identifying the files
    that are added/updated as part of the current build and store the information in a file 'ListOfCommitItems.txt'.
    The script itself does not do any deduplication etc.

    The script uses the Azure devops api (https://github.com/Microsoft/azure-devops-python-api) to get the information.
'''

from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
import logging
from types import SimpleNamespace
import os

__VERSION__ = "1.0.0"
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

def build_context(p_azdevops_uri ,p_auth_token ,p_project):
    context = SimpleNamespace()

    context.project_of_interest = p_project
    logger.info("Creating azcli connection ...")
    context.connection = Connection(
        base_url=p_azdevops_uri,
        creds=BasicAuthentication('PAT', p_auth_token),
        user_agent='azgitdiff/' + __VERSION__)

    return context

'''
    Gets the list of changes related to the identified build
'''
def get_changes_on_bld(p_context ,p_project_id ,p_bld_id ,p_bld_number):
    logger.info("Retrieving changes made on build : %s %s..." ,p_bld_id  ,p_bld_number)
    build_client = p_context.connection.clients.get_build_client()
    changes = build_client.get_build_changes(p_project_id ,p_bld_id )

    chgs = []
    for chg in changes.value:
        chg.build_id = p_bld_id
        chg.build_number = p_bld_number
        chgs.append(chg)

    return chgs

def get_commit_changes_for_build(p_context ,p_project_id ,p_bld_id ,p_bld_number ,p_commit_id ,p_repository_name):
    logger.info("Retrieving commits for build : %s %s commit: %s..." ,p_bld_id  ,p_bld_number ,p_commit_id)
    git_client = p_context.connection.clients.get_git_client()
    comt = git_client.get_changes(p_commit_id ,repository_id=p_repository_name ,project=p_project_id)

    commits = []
    for chg in comt.changes:
        x = {}
        x['commit_id'] = p_commit_id
        x['item_path'] = '.' + chg['item']['path']
        commits.append(x)

    return commits

def savefiles_toexecute(filepath ,execution_fl_arrays):
    logger.info("Saving the identified commit items information at : %s ...",filepath)
    with open(filepath, 'w') as out:
        for cmt in execution_fl_arrays:
            out.write("{},{}\n".format(cmt['commit_id'] ,cmt['item_path']) )


if __name__ == "__main__":
    '''
      various informations like reponame, projectid etc are retrieved from the env. THe azure build pipelines sets
      the value at runtime. 
    '''
    azdevops_uri = os.environ.get('SYSTEM_TEAMFOUNDATIONCOLLECTIONURI')
    azdevops_project = os.environ.get('SYSTEM_TEAMPROJECT')
    azdevops_projectid = os.environ.get('SYSTEM_TEAMPROJECTID')

    azdevops_repository_name = os.environ.get('BUILD_REPOSITORY_NAME')
    azdevops_repository_id = os.environ.get('BUILD_REPOSITORY_ID')

    azdevops_build_id = os.environ.get('BUILD_BUILDID')
    azdevops_build_number = os.environ.get('BUILD_BUILDNUMBER')
    access_token = os.environ.get('SYSTEM_ACCESSTOKEN')

    context = build_context(azdevops_uri ,access_token ,azdevops_project)

    commits_on_build = get_changes_on_bld(context ,azdevops_projectid ,azdevops_build_id ,azdevops_build_number)

    listof_commits=[]
    for bld_commit in commits_on_build:
        #logger.info("Change => build : %s %s commit: %s..." ,azdevops_build_id  ,azdevops_build_number ,bld_commit.id)
        t = get_commit_changes_for_build(context ,azdevops_projectid ,azdevops_build_id ,azdevops_build_number ,bld_commit.id ,azdevops_repository_name)
        listof_commits.extend(t)

    artifact_staging_dir = os.environ.get('SYSTEM_ARTIFACTSDIRECTORY')
    savefiles_toexecute(artifact_staging_dir + "/ListOfCommitItems.txt" ,listof_commits)

    logger.info("Finished!!!")