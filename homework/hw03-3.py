from mrjob.job import MRJob

class Avgcoolrating(MRJob):

    @staticmethod
    def count(values):
        tot_rating_list = []
        tot_rating = 0
        for i in values:
            tot_rating += i
            tot_rating_list.append(i)
        return len(tot_rating_list),tot_rating


    def mapper(self, _, line):
        data = line.split(',')
        star_rating = data[3]
        cool = data[7]
        if cool != 0 and star_rating != 'stars':
            yield "Rating",int(star_rating)

    def reducer(self,key,values):
        coolcount,total_rating = self.count(values)
        yield "Average rating for cool not equal to 0 : ",(total_rating/coolcount)

if __name__ == '__main__':
    Avgcoolrating()

mr_job = Avgcoolrating(args=['yelp.csv'])
with mr_job.make_runner() as runner:
    runner.run()
    for key, value in mr_job.parse_output(runner.cat_output()):
        print(key, value)
