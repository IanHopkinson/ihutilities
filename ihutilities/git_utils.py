#!/usr/bin/env python
# encoding: utf-8
from __future__ import unicode_literals

import subprocess
    
def git_sha(repo_dir):
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