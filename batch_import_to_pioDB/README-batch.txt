To run the script you need:


1.) python interpreter

2.) PostgreSQL containing data

3.) direnv to hold the secret keys

4.) pio database inside PostgreSQL to import the extracted data

Code formatting is quite readable. Read code and comments to understand the script.



Note:
 
- This script creates a JSON file before importing to pio database.
 
- This JSON file may be stored for further usage, or must be deleted manually.