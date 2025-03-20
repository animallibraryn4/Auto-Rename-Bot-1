#!/bin/bash
set -e

# Update package list and install ffmpeg
apt-get update
apt-get install -y ffmpeg

# Install Python dependencies
pip install -r requirements.txt

# Run the main application
python plugins/file_rename.py
