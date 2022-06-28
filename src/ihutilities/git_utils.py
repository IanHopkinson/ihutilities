#!/usr/bin/env python
# encoding: utf-8

import subprocess
    
def git_sha(repo_dir):
    """This function returns the short SHA-1 hash of a repo

    Args:
       repo_dir (str):
            A string containing the repo_dir of interest

    Returns:
       A string containing the short SHA-1 hash of the repo

    Example:
        >>> sha = git_sha(".")

    """

    cmd = ['git','rev-parse','--short', 'HEAD']
    pr = subprocess.Popen(cmd,                  
            cwd=repo_dir, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            shell=False)
    (out, error) = pr.communicate()

    if len(error) > 0:
        print(error)

    return out.strip().decode("utf-8")

def git_uncommitted_changes(filename, repo_dir):
    """
    This checks the committed state of a file in a repo

    Args:
       repo_dir (str):
            A string containing the repo_dir of interest
       filename (str):
            A string containing the filename of interest. A empty or blank string
            indicates the committed state for the whole repository

    Returns:
       True if there are uncommitted changes on the requested filename

    Example:
        >>> sha = git_sha(".")

    """

    if filename == ' ' or filename == '':
        filename = '--'
    cmd = ['git', 'diff', filename]
    pr = subprocess.Popen(cmd,                  
            cwd=repo_dir, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            shell=False)
    (out, error) = pr.communicate()

    if len(error) > 0:
        print(error)

    if len(out) > 0:
        return True
    else:
        return False

def git_calculate_file_sha(filepath):
    """This function returns the full SHA-1 hash of file, it does not require the file to be committed

    Args:
       filepath (str):
            A string containing the filepath

    Returns:
       A string containing the SHA-1 hash of the repo

    Example:
        >>> sha = git_sha(".")

    """

    cmd = ['git','hash-object',filepath]
    pr = subprocess.Popen(cmd,                  
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            shell=False)
    (out, error) = pr.communicate()

    if len(error) > 0:
        print(error)

    return out.strip().decode("utf-8")

def git_describe(repo_dir):
    """This function returns the git describe --tags output

    Args:
       repo_dir (str):
            A string containing the repo_dir of interest

    Returns:
       A string containing the output of git describe --tags for the specified repository directory

    Example:
        >>> description = git_describe(".")

    """

    cmd = ['git','describe','--tags']
    pr = subprocess.Popen(cmd,                  
            cwd=repo_dir, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            shell=False)
    (out, error) = pr.communicate()

    if len(error) > 0:
        print(error)

    return out.strip().decode("utf-8")

def git_check_up_to_date(repo_dir):
    up_to_date = False
    cmd = ["git", "fetch", "--dry-run"]
    pr = subprocess.Popen(cmd,                  
            cwd=repo_dir, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            shell=False)
    (out, error) = pr.communicate()

    if len(out) == 0 and len(error) == 0:
        up_to_date = True
    else:
        print("Git repo not up to date", flush=True)
        print("Error: '{}'".format(error), flush=True)
        print("Message: '{}'".format(out), flush=True)


    return up_to_date
