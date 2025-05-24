#!/usr/bin/env python3
"""
Debug wrapper script to catch and identify DataFrame truth value errors
"""
import sys
import traceback
import pandas as pd

def check_dataframe_boolean_usage():
    """Replace the pandas DataFrame.__nonzero__ method to catch the error location"""
    original_nonzero = pd.DataFrame.__nonzero__
    
    def debug_nonzero(self):
        print("\n*** CRITICAL ERROR: DataFrame truth value check detected! ***")
        print("This error occurs when a DataFrame is used in a boolean context, like 'if df:'")
        print("Stack trace at the error point:")
        traceback.print_stack()
        print("\nUse `if not df.empty:` or `if len(df) > 0:` instead of `if df:`")
        
        # Call the original method which will raise the ValueError
        return original_nonzero(self)
    
    # Replace the method
    pd.DataFrame.__nonzero__ = debug_nonzero
    print("DataFrame boolean context checker installed")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 debug_wrapper.py <script_to_run.py>")
        sys.exit(1)
    
    # Install the debug helper
    check_dataframe_boolean_usage()
    
    # Load the script specified by the user
    script_path = sys.argv[1]
    script_args = sys.argv[2:]
    
    print(f"Running {script_path} with debug wrappers...")
    
    # Save original argv and restore it for the script
    original_argv = sys.argv
    sys.argv = [script_path] + script_args
    
    # Execute the script
    with open(script_path) as f:
        script_code = f.read()
    
    exec(script_code, {'__file__': script_path, '__name__': '__main__'})