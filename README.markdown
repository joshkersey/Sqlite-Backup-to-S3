# SQLite to Amazon S3 backup script

**Requirements:**

* Python
* Boto (http://code.google.com/p/boto/)

# The Problem

We needed to automate the backup of our SQLite databases.  We have quite a few sites that run on SQLite and our managed backup plan only handles MySQL.

# The Solution

This script takes a given directory and walks through those directories looking for files with a .db extension.  Those files are then uploaded to a bucket on Amazon S3.

I've also added a function to remove databases that are older than X days.  We've chosen to keep monthly backups from the first day of each month for our archive.

# Usage

I dropped this into a cron job that runs every night.  Be sure that the user running the cron job has read permission on the target backup directory.
