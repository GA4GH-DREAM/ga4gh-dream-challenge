##-----------------------------------------------------------------------------
##
## challenge specific code and configuration
##
##-----------------------------------------------------------------------------
import os
import time
import traceback
import subprocess
import json
import re
from datetime import datetime
from StringIO import StringIO
from synapseclient import Folder, File, Wiki
import shutil
import synapseutils as synu
import zipfile
import yaml
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
        'handle': 'md5sum',
        'param_ext': '.json',
        'report_src': 'syn10167920',
        'report_dest': 'syn10163084',
    },
    {
        'id':9603665,
        'handle': 'hello_world',
        'param_ext': '.json',
        'report_src': 'syn9630940',
        'report_dest': 'syn10163081',
    },
    {
        'id':9604287,
        'handle': 'biowardrobe_chipseq_se',
        'param_ext': '.json',
        'report_src': 'syn9772359',
        'report_dest': 'syn10163082',
    },
    {
        'id':9604596,
        'handle': 'gdc_dnaseq_transform',
        'param_ext': '.json',
        'report_src':'syn9766994',
        'report_dest': 'syn10156701',
    },
    {   'id':9605240,
        'handle': 'bcbio_NA12878-chr20',
        'param_ext': '.json',
        'report_src': 'syn9725771',
        'report_dest': 'syn10163083',
    },
    {   'id':9605639,
        'handle': 'encode_mapping_workflow',
        'param_ext': '.json',
        'report_src': 'syn10163025',
        'report_dest': 'syn10240069',
    },
    {   'id':9606345,
        'handle': 'knoweng_gene_prioritization',
        'param_ext': '.yml',
        'report_src': 'syn10235824',
        'report_dest': 'syn10611690',
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

    # number and organization outputs will vary for each queue; no simple way
    # to validate file presence -- can leave it up to the checker
    # assert all([os.path.exists(os.path.join(submissionDir, actualName)) for actualName in config['filename']]), "Your submitted file or zipped file must contain these file(s): %s" % ",".join(config['filename'])

    scriptDir = os.path.dirname(os.path.realpath(__file__))
    checkerDir = os.path.join(scriptDir, 'checkers')
    knowngoodDir = os.path.join(scriptDir, 'known_goods')
    outputDir = os.path.join(scriptDir, 'output')

    # clear existing outputs
    for f in os.listdir(outputDir):
        os.remove(os.path.join(outputDir, f))

    # check whether checker cwl and param file are present
    checkerPath = os.path.join(checkerDir, annotations['workflow'] + '_checker.cwl')
    origCheckerParamPath = checkerPath + config['param_ext']
    if not os.path.exists(checkerPath) and os.path.exists(origCheckerParamPath):
        raise ValueError("Must have these cwl and param files: %s, %s" % (checkerPath, origCheckerParamPath))

    # link checker param file to submission folder
    newCheckerParamPath = os.path.join(submissionDir, annotations['workflow'] + '_checker.cwl' + config['param_ext'])
    if not os.path.exists(newCheckerParamPath):
        if config['handle'] == 'encode_mapping_workflow':
            shutil.copy(origCheckerParamPath, newCheckerParamPath)
            sed_command = ['sed', '-i', '-e', 's|path.*\"\"|path\": \"{}\"|g'.format(submissionDir), newCheckerParamPath]
            print(' '.join(sed_command))
            subprocess.call(sed_command)
        else:
            os.symlink(origCheckerParamPath, newCheckerParamPath)

    # link known good outputs to submission folder
    knowngoodPaths = os.listdir(os.path.join(knowngoodDir, annotations['workflow']))
    for knowngood in knowngoodPaths:
        origKnowngoodPath = os.path.join(knowngoodDir, annotations['workflow'], knowngood)
        newKnowngoodPath = os.path.join(submissionDir, knowngood)
        if not os.path.exists(newKnowngoodPath):
            os.symlink(origKnowngoodPath, newKnowngoodPath)

    # run checker
    validate_cwl_command = ['/home/ubuntu/.local/bin/cwl-runner', '--non-strict', '--outdir', outputDir, checkerPath, newCheckerParamPath]
    print("Running checker with command\n{}\n...".format(' '.join(validate_cwl_command)))
    try:
        subprocess.call(validate_cwl_command)
    except OSError as ex:
        print "Exception from 'cwl-runner':", type(ex), ex, ex.message
        raise

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
        raise AssertionError("Your submitted output(s) appear to be incorrect (i.e., did not pass all tests in the {} checker tool). Please go to this folder: https://www.synapse.org/#!Synapse:{} to look at your log and result files".format(annnotations['workflow'], subFolder))

    return True, "Submission validated, ready for documentation!"

def _initialize_report(syn, evaluation, submission):
    """
    Create a dummy Synapse entity and attach wiki for report.
    """
    config = evaluation_queue_by_id[int(evaluation.id)]
    print("wiki source: {}".format(config['report_src']))
    print("wiki target: {}".format(config['report_dest']))
    template_wiki = syn.getWiki(config['report_src'])
    report_folder = syn.get(config['report_dest'], downloadFile=False)
    scriptDir = os.path.dirname(os.path.realpath(__file__))
    reportDir = os.path.join(scriptDir, 'reports')

    # create and store report file
    report_path = os.path.join(reportDir, '{}_README'.format(submission.id))
    report_msg = """Submission report for object '{}' in evaluation queue '{}'. 
    This file is a placeholder; see attached Synapse wiki for full report.
    """.format(submission.id, submission.evaluationId)
    with open(report_path, 'w') as f:
        f.write(report_msg)
    report_file = syn.store(File(report_path, parentId=report_folder.id))

    # copy template wiki to report file
    try:
        report_wiki = syn.getWiki(report_file.id)
    except:
        report_wiki = Wiki(owner = report_file.id)
    report_wiki.markdown = template_wiki.markdown
    report_wiki = syn.store(report_wiki)

    return 'INITIALIZED', report_file.id


def validate_submission_report(syn, evaluation, submission, status_annotations, dry_run=False):
    config = evaluation_queue_by_id[int(evaluation.id)]
    # get submission report wiki
    report_msg = "Report is ready to edit."
    new_report = False
    try:
        report_status, report_id = (
            status_annotations['reportStatus'],
	    status_annotations['reportEntityId']
	)
        report_wiki = syn.getWiki(report_id)
    except:
        report_status, report_id = _initialize_report(syn, evaluation, submission)
        report_wiki = syn.getWiki(report_id)
        new_report = True
        if dry_run:
            syn.delete(report_id)

    print('checking report update time')
    created_time = datetime.strptime(report_wiki['createdOn'], '%Y-%m-%dT%H:%M:%S.%fZ')
    modified_time = datetime.strptime(report_wiki['modifiedOn'], '%Y-%m-%dT%H:%M:%S.%fZ')

    required_fields = ['name', 'institution', 'platform', 'workflow_type',
                       'runner_version', 'docker_version', 'environment',
                       'env_cpus', 'env_memory', 'env_disk']
    platform = 'pending documentation'
    environment = 'pending documentation'
    if modified_time > created_time:
        report_msg = "Report appears to have been modified since creation date/time and is in progress."
        print('checking report')
        # validate report
        report_dict = _parse_wiki_yaml(report_wiki.markdown)
        platform = report_dict['platform']
        environment = report_dict['environment']

        missed_fields = [f for f in required_fields if f not in report_dict]
        assert not len(missed_fields), "The following fields are missing from your report: {}; please refer to the original template wiki for the {} workflow here to ensure all fields are present: https://www.synapse.org/#!Synapse:{}".format(missed_fields, config['handle'], config['report_src']) 
        empty_fields = [f for f in required_fields
                         if not len(report_dict[f])]
        if len(empty_fields):
            report_status = 'IN_PROGRESS'
            report_msg = 'The following fields are still missing values: {}'.format(missed_fields)
        else:
            report_status = 'VALIDATED'
            report_msg = "Report is pending final review and approval."

    return {'reportStatus': report_status, 'reportEntityId': report_id, 'platform': platform, 'environment': environment}, report_msg, new_report


def score_submission(evaluation, submission):
    """
    Find the right scoring function and score the submission

    :returns: (score, message) where score is a dict of stats and message
              is text for display to user
    """
    config = evaluation_queue_by_id[int(evaluation.id)]
    #Make sure to round results to 3 or 4 digits
    return (dict(), "You did fine!")

def _parse_wiki_yaml(wiki_markdown):
    """
    Parse YAML fields from code chunks in a wiki markdown and return dict.
    """
    md_lines = StringIO(wiki_markdown).readlines()
    code_chunks = [(idx, l) for idx, l in enumerate(md_lines)
                   if re.search('```', l)]
    yaml_lines = []
    for idx, chunk in enumerate(code_chunks[0:-1]):
        if re.search('YAML', chunk[1]):
            yaml_lines += md_lines[chunk[0]+1:code_chunks[idx+1][0]]
    return yaml.load(''.join(yaml_lines))
