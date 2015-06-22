import argparse
import sys
from runner import Runner
import lib.mixpanel_data_puller as puller
from retrying import retry
from check_errors import retry_if_value_error

class SerialRunner(Runner):

    def create_parser(self):
        parser = argparse.ArgumentParser(description='Serial Runner for mixpanel data pull.')
        return parser

    def pull_data(self, date):
        if self.args.dry:
            return "DRY_MODE"
        return puller.pull(date, date, self.args.apikey, self.args.apisecret, self.args.events)

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=10, retry_on_exception=retry_if_value_error)
    def get_and_write(self, date):
        print "Pulling data for %s" % date
        request_url = puller.get_url(date, date, self.args.apikey, self.args.apisecret)
        print request_url
        data_iter = self.pull_data(date)
        s3_output_file = "%s%s" % (self.output_bucket, date)
        try:
            self.put_s3_string_iter(data_iter, s3_output_file, request_url, zip=True)
        except(ValueError), e:
            error_logger(str(e))
            raise e

    def pull_data_for_date_range(self):
        start_date, end_date = self.args.startdate, self.args.enddate
        for date in self.date_iter(start_date, end_date):
            date = puller.stringify_date(date)
            try:
                self.get_and_write(date)
            except(ValueError):
                continue

def error_logger(string):
    with file("bad_requests.txt", 'a') as f:
        f.write(string + '\n')

def run(argv):
    runner = SerialRunner()
    runner.parse_args(argv)
    runner.pull_data_for_date_range()

if __name__ == '__main__':
    run(sys.argv)
