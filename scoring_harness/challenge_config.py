##-----------------------------------------------------------------------------
##
## challenge specific code and configuration
##
##-----------------------------------------------------------------------------
import os
import subprocess
import json
from synapseclient import Folder, File
import shutil

## A Synapse project will hold the assetts for your challenge. Put its
## synapse ID here, for example
## CHALLENGE_SYN_ID = "syn1234567"
CHALLENGE_SYN_ID = "syn8507133"

## Name of your challenge, defaults to the name of the challenge's project
CHALLENGE_NAME = "GA4GH-DREAM Tool Execution Challenge"

## Synapse user IDs of the challenge admins who will be notified by email
## about errors in the scoring script
ADMIN_USER_IDS = ['3324230','2223305']

evaluation_queues = [
#GA4GH-DREAM_md5sum (9603664)
#GA4GH-DREAM_hello_world (9603665)
    {
        'id':9603664,
        'filename':'md5sum.result'
    },
    {
        'id':9603665,
        'filename':'hello_world.result'
    }
]
evaluation_queue_by_id = {q['id']:q for q in evaluation_queues}


## define the default set of columns that will make up the leaderboard
LEADERBOARD_COLUMNS = [
    dict(name='objectId',      display_name='ID',      columnType='STRING', maximumSize=20),
    dict(name='userId',        display_name='User',    columnType='STRING', maximumSize=20, renderer='userid'),
    dict(name='entityId',      display_name='Entity',  columnType='STRING', maximumSize=20, renderer='synapseid'),
    dict(name='versionNumber', display_name='Version', columnType='INTEGER'),
    dict(name='name',          display_name='Name',    columnType='STRING', maximumSize=240),
    dict(name='team',          display_name='Team',    columnType='STRING', maximumSize=240)]

## Here we're adding columns for the output of our scoring functions, score,
## rmse and auc to the basic leaderboard information. In general, different
## questions would typically have different scoring metrics.
leaderboard_columns = {}
for q in evaluation_queues:
    leaderboard_columns[q['id']] = LEADERBOARD_COLUMNS + [
        dict(name='score',         display_name='Score',   columnType='DOUBLE'),
        dict(name='rmse',          display_name='RMSE',    columnType='DOUBLE'),
        dict(name='auc',           display_name='AUC',     columnType='DOUBLE')]

## map each evaluation queues to the synapse ID of a table object
## where the table holds a leaderboard for that question
leaderboard_tables = {}


def validate_submission(syn, evaluation, submission, annotations):
    """
    Find the right validation function and validate the submission.

    :returns: (True, message) if validated, (False, message) if
              validation fails or throws exception
    """
    config = evaluation_queue_by_id[int(evaluation.id)]
    fileName = os.path.basename(submission.filePath)
    scriptDir = os.path.dirname(os.path.realpath(__file__))
    outputDir = os.path.join(scriptDir, "output")
    resultFile = os.path.join(outputDir,'results.json')
    logFile = os.path.join(outputDir,'log.txt')
    submissionDir = os.path.dirname(submission.filePath)
    assert config['filename'] == fileName, "Your submitted file must be named: %s, not %s" % (config['filename'],fileName)
    checkerPath = os.path.join(scriptDir, "checkers", annotations['workflow'] + "_checker.cwl")
    origCheckerJsonPath = checkerPath + ".json"
    shutil.copy(origCheckerJsonPath, submissionDir)
    newCheckerJsonPath = os.path.join(submissionDir, annotations['workflow'] + "_checker.cwl.json")
    validate_cwl_command = ['cwl-runner','--outdir',outputDir,checkerPath,newCheckerJsonPath]
    subprocess.call(validate_cwl_command)
    with open(resultFile) as data_file:    
        results = json.load(data_file)

    if results['Overall'] == False:
        subFolder = syn.store(Folder(submission.id,parent="syn9856439"))
        resultFileEnt = syn.store(File(resultFile, parent=subFolder))
        logFileEnt = syn.store(File(logFile,parent=subFolder))
        syn.setPermissions(subFolder, annotations['team'], access=["READ","DOWNLOAD"])
        raise AssertionError("Your resulting file is incorrect, please go to this folder: https://www.synapse.org/#!Synapse:%s to look at your log and result files" % subFolder.id)

    return True, "You passed!"


def score_submission(evaluation, submission):
    """
    Find the right scoring function and score the submission

    :returns: (score, message) where score is a dict of stats and message
              is text for display to user
    """
    config = evaluation_queue_by_id[int(evaluation.id)]
    #Make sure to round results to 3 or 4 digits
    return (dict(), "You did fine!")


