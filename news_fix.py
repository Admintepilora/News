#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to find and fix the remaining issues in the news scraper scripts
"""
import os
import re

def check_file_for_issues(filepath):
    print(f"Checking {filepath}...")
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Look for any remaining direct DataFrame evaluations that might cause errors
    df_truth_checks = re.findall(r'if\s+\w+_df[^:\n.]*?(?!\.empty|\.any\(\)|\.all\(\)|len\(\w+_df\)|\.size):', content)
    print(f"Found {len(df_truth_checks)} potential DataFrame truth value issues:")
    for i, match in enumerate(df_truth_checks):
        print(f"{i+1}. {match}")
    
    return df_truth_checks

def main():
    base_dir = "/home/tepilora/Github/Admintepilora/News"
    scripts = [
        'DuckDuckGoApiNews.py',
        'GNewsApiNews.py',
        'WebSitesNews.py'
    ]
    
    for script in scripts:
        script_path = os.path.join(base_dir, script)
        issues = check_file_for_issues(script_path)
        if issues:
            print(f"Issues found in {script}")
        else:
            print(f"No issues found in {script}")

if __name__ == "__main__":
    main()