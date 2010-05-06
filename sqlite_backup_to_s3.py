import os
from datetime import date, time, timedelta

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError

class S3_Manager(object):
    
    def __init__(self):
        """
        self.ignore is a list of directory strings you want to ignore at
        any level of the database_directory.
        
        Example:
        Directory = "/usr/local/test/"
        To ignore all files and other directories in the 'test' directory:
            self.ignore = ['test']
        
        To ignore all files and other directories in the 'usr' directory:
            self.ignore = ['usr']
        
        """
        self.database_directory = "<directory_to_search>"
        self.bucket = "<S3_bucket_name>"
        self.aws_access_key = "<AWS_access_key>"
        self.aws_secret_key = "<AWS_secret_key>"
        self.database_expiry_date = date.today() - timedelta(days=30)
        self.ignore = []
        
    def get_connection(self):
        """
        Initializes S3 connection.
        """
        return S3Connection(self.aws_access_key, self.aws_secret_key)
        
    def remove_old_db(self, conn):
        """
        Removes database files that are older than specified date range.
        We chose to keep files from the first day of every month for our
        archive.
        """
        bucket = conn.get_bucket(self.bucket)
        keys = bucket.list()
        for k in keys:
            kd = k.name[-10:]
            key_date = date(int(kd[:4]), int(kd[5:7]), int(kd[-2:]))
            if key_date < self.database_expiry_date and key_date.day != 1:
                k.delete()
                
    def backup_db(self, conn):
        """
        Retrieve list of files to backup.  Appends _(today's date) to original
        filename.  This allows the remove_old_db function to check file dates.
        
        Example:
        test.db would become test.db_2010-03-01
        """
        bucket = conn.get_bucket(self.bucket)
        file_list = self.find_files()
        for item in file_list:
            key_name = item['filename'] + "_%s" % date.today()
            key = bucket.new_key(key_name)
            key.set_metadata('Content-Type', 'text/plain')
            try:
                upload = key.set_contents_from_filename(item['path'], policy='private')
            except S3ResponseError:
                # Handle errors returned from AWS here
                
    def find_files(self):
        """
        Walk through the given directory to find every *.db file.  If *.db
        file is found append it's path to the file_list.
        """
        file_list = []
        for root, dirs, files in os.walk(self.database_directory):
            for d in self.ignore:
                if d in dirs:
                    dirs.remove(d)
            for f in files:
                extension = os.path.splitext(f)[1]
                if extension == ".db":
                    full_path = os.path.join(root, f)
                    data = {'path': full_path, 'filename': f}
                    file_list.append(data)
        return file_list
            
        
if __name__=="__main__":
    manager = S3_Manager()
    conn = manager.get_connection()
    manager.remove_old_db(conn)
    manager.backup_db(conn)
    


