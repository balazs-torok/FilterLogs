filterlogs.py

Filters log files. It is written in Python 3.6.

Parameters:

- Source directory
- Target directory
- Configuration file path
- Number of parallel processes to run
- Number of files to be processed in one parallel run

Example:

./filterlogs.py /var/log output configuration.txt 8 1024
