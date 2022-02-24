from mrjob.job import MRJob

class Countmonth(MRJob):

    def mapper(self, _, line):
        data = line.split(',')
        full_date = data[1]
        year_month = full_date[:7]
        if full_date != 'date':
            yield year_month, 1

    def reducer(self,key,values):
        yield key,sum(values)

if __name__ == '__main__':
    Countmonth()

mr_job = Countmonth(args=['yelp.csv'])
with mr_job.make_runner() as runner:
    runner.run()
    for key, value in mr_job.parse_output(runner.cat_output()):
        print(key, value)
