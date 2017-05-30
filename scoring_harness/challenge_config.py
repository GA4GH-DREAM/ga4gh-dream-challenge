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
import synapseutils as synu
import zipfile
## A Synapse project will hold the assetts for your challenge. Put its
## synapse ID here, for example
## CHALLENGE_SYN_ID = "syn1234567"
CHALLENGE_SYN_ID = "syn8507133"

## Name of your challenge, defaults to the name of the challenge's project
CHALLENGE_NAME = "GA4GH-DREAM Tool Execution Challenge"

## Synapse user IDs of the challenge admins who will be notified by email
## about errors in the scoring script
ADMIN_USER_IDS = ['3324230','2223305']

CHALLENGE_OUTPUT_FOLDER = "syn9856439"
evaluation_queues = [
#GA4GH-DREAM_md5sum (9603664)
#GA4GH-DREAM_hello_world (9603665)
    {
        'id':9603664,
        'filename':['md5sum.result']
    },
    {
        'id':9603665,
        'filename':['hello_world.result']
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
    submissionDir = os.path.dirname(submission.filePath)
    if submission.filePath.endswith('.zip'):
        zip_ref = zipfile.ZipFile(submission.filePath, 'r')
        zip_ref.extractall(submissionDir)
        zip_ref.close()

    # assert all([os.path.exists(os.path.join(submissionDir, actualName)) for actualName in config['filename']]), "Your submitted file or zipped file must contain these file(s): %s" % ",".join(config['filename'])

    scriptDir = os.path.dirname(os.path.realpath(__file__))
    checkerDir = os.path.join(scriptDir, 'checkers')
    knowngoodDir = os.path.join(scriptDir, 'known_goods')
    outputDir = os.path.join(scriptDir, 'output')

    # check whether checker cwl and json are present
    checkerPath = os.path.join(checkerDir, annotations['workflow'] + '_checker.cwl')
    origCheckerJsonPath = checkerPath + ".json"
    if not os.path.exists(checkerPath) and os.path.exists(origCheckerJsonPath):
        raise ValueError("Must have these cwl and json files: %s, %s" % (checkerPath, origCheckerJsonPath))

    # link checker json to submission folder
    newCheckerJsonPath = os.path.join(submissionDir, annotations['workflow'] + '_checker.cwl.json')
    if not os.path.exists(newCheckerJsonPath):
        # shutil.copy(origCheckerJsonPath, submissionDir)
        os.symlink(origCheckerJsonPath, newCheckerJsonPath)

    # link known good outputs to submission folder
    knowngoodPaths = os.listdir(os.path.join(knowngoodDir, annotations['workflow']))
    for knowngood in knowngoodPaths:
        origKnowngoodPath = os.path.join(knowngoodDir, annotations['workflow'], knowngood)
        newKnowngoodPath = os.path.join(submissionDir, knowngood)
        if not os.path.exists(newKnowngoodPath):
            os.symlink(origKnowngoodPath, newKnowngoodPath)

    # run checker
    validate_cwl_command = ['cwl-runner', '--non-strict', '--outdir', outputDir, checkerPath, newCheckerJsonPath]
    print(validate_cwl_command)
    subprocess.call(validate_cwl_command)

    # collect checker results
    resultFile = os.path.join(outputDir,'results.json')
    logFile = os.path.join(outputDir,'log.txt')
    with open(resultFile) as data_file:
        results = json.load(data_file)

    try:
        overall_status = results['overall']
    except KeyError:
        overall_status = results['Overall']
    except KeyError:
        print("No 'overall' field found in {}".format(resultFile))

    if not overall_status:
        output = synu.walk(syn, CHALLENGE_OUTPUT_FOLDER)
        outputFolders = output.next()[1]
        outputSynId = [synId for name, synId in outputFolders if str(submission.id) == name]
        if len(outputSynId) == 0:
            subFolder = syn.store(Folder(submission.id,parent=CHALLENGE_OUTPUT_FOLDER)).id
        else:
            subFolder = outputSynId[0]
        resultFileEnt = syn.store(File(resultFile, parent=subFolder))
        if os.stat(logFile).st_size > 0:
            logFileEnt = syn.store(File(logFile,parent=subFolder))
        for participant in submission.contributors:
            if participant['principalId'] in ADMIN_USER_IDS: 
                access = ['CREATE', 'DOWNLOAD', 'READ', 'UPDATE', 'DELETE', 'CHANGE_PERMISSIONS', 'MODERATE', 'CHANGE_SETTINGS']
            else:
                access = ['READ','DOWNLOAD']
            syn.setPermissions(subFolder, principalId = participant['principalId'], accessType = access)
        raise AssertionError("Your resulting file is incorrect, please go to this folder: https://www.synapse.org/#!Synapse:%s to look at your log and result files" % subFolder)

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


