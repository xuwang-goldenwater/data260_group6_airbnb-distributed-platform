"""Kept for old instructions: same as `run_demo.py three-debates`. Use run_demo.py directly."""
import runpy
import sys
from pathlib import Path

if __name__ == "__main__":
    sys.argv = [sys.argv[0], "three-debates", *sys.argv[1:]]
    runpy.run_path(str(Path(__file__).with_name("run_demo.py")), run_name="__main__")
