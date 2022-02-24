from mrjob.job import MRJob

class Avgwords(MRJob):

    @staticmethod
    def count(values):
        wordcnt_list = []
        wordcnt = 0
        for i in values:
            wordcnt += i
            wordcnt_list.append(i)
        return len(wordcnt_list),wordcnt


    def mapper(self, _, line):
        data = line.split(',')
        text = data[4]
        words_split = text.split()
        wordcount = len(words_split)
        if words_split != 'text':
            yield "Wordcount",wordcount

    def reducer(self,key,values):
        rowcount,total_wordcount = self.count(values)
        yield "Average words per review is ",(total_wordcount/rowcount)

if __name__ == '__main__':
    Avgwords()

mr_job = Avgwords(args=['yelp.csv'])
with mr_job.make_runner() as runner:
    runner.run()
    for key, value in mr_job.parse_output(runner.cat_output()):
        print(key, value)
