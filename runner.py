import argparse
from lib.mixpanel_data_puller import extract_dates, stringify_date, parse_date
from datetime import timedelta
import md5
import subprocess
import datetime
import os

class Runner:

    def create_base_args(self, parser):
        parser.add_argument('--bucket', required=True,
                            help='s3 bucket')
        parser.add_argument('--apikey', required=True,
                            help='mixpanel api key')
        parser.add_argument('--apisecret', required=True,
                            help='mixpanel api secret')
        parser.add_argument('--startdate', required=True, type=parse_date,
                            help='start date')
        parser.add_argument('--enddate', required=True, type=parse_date,
                            help='end date')
        parser.add_argument('--tmpdir', default="/tmp",
                            help='Temporary directory')
        parser.add_argument('--dry', default=False, dest='dry', action='store_true',
                            help='dry mode')
        parser.add_argument('--minimum-time', type=int, required=False, dest='minimum_time', help='if the API doesn\'t take minimum_time in seconds, try again')

    def parse_args(self, argv):
        parser = self.create_parser()
        self.create_base_args(parser)

        self.args = parser.parse_args()

        self.bucket = self.args.bucket
        if self.bucket[-1] != '/':
            self.bucket += '/'
        self.input_bucket = "%sinput/" % self.bucket
        self.code_bucket = "%scode/" % self.bucket
        self.emr_code_dir = "/mnt/code/"
        self.output_bucket = "%soutput/" % self.bucket

    def rm(self, filename):
        self.run_command(['rm', '-f', filename])

    def gzip(self, filename):
        gz_filname = "%s.gz" % filename
        self.run_command(['gzip', filename])
        return gz_filname

    def run_command(self, cmd):
        print "Running: %s" % " ".join(cmd)
        if self.args.dry:
            return
        exit_code = subprocess.call(cmd)
        if exit_code != 0:
            raise Exception("Error: Exit code %d found for command: %s" % (exit_code, cmd))
    
    def temp_filename(self):
        m = md5.new()
        m.update(str(self.args.startdate))
        m.update(str(self.args.enddate))
        return m.digest()

    def put_s3_string_iter(self, string_iter, s3_filename, request_url, zip=False):
        start = datetime.datetime.utcnow()
        tmp_file = "%s/%s.txt" % (self.args.tmpdir, self.temp_filename())
        f = open(tmp_file, 'w')
        for string in string_iter:
            f.write(string)
        f.close()

        file_size = os.stat(tmp_file).st_size
        seconds = (datetime.datetime.utcnow() - start).total_seconds()
        if seconds < self.args.minimum_time:
            error_string = '\t'.join([str(start), str(seconds), str(file_size), str(request_url)])
            raise ValueError(error_string)

        if zip:
            tmp_file = self.gzip(tmp_file)
            s3_filename = "%s.gz" % s3_filename
        print "Writing to s3"
        self.put_s3_file(tmp_file, s3_filename)
        self.rm(tmp_file)

    def put_s3_string(self, string, s3_filename, zip=False):
        def string_iter():
            yield string
        self.put_s3_string_iter(string_iter, s3_filename, zip)

    def put_s3_file(self, filename, bucket):
        self.run_command(("s3cmd put -r %s s3://%s" % (filename, bucket)).split())

    def date_iter(self, start_date, end_date):
        while start_date <= end_date:
            yield start_date
            start_date += datetime.timedelta(days=1)

