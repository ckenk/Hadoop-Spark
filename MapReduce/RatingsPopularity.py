from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsPopularity(MRJob):
    def steps(self):
        return [
            # MRStep(mapper=self.mapper_get_ratings, reducer=self.reducer_count_ratings)
            MRStep(mapper=self.mapper_get_popularity, reducer=self.reducer_count_popularity),
            MRStep(reducer=self.reducer_sorted_output)
        ]

    # def mapper_get_ratings(self, _, line):
    #     (userID, movieID, rating, timestamp) = line.split('\t')
    #     yield movieID, 1
    #
    # def reducer_count_ratings(self, key, values):
    #     yield key, sum(values)

    def mapper_get_popularity(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_popularity(self, key, values):
        # yield key, sum(values)
        yield str(sum(values)).zfill(5), key

    # here `movies` is plural because there can be different movies that share the same popularity
    def reducer_sorted_output(self, pop_count, movies):
        for movie in movies:
            yield movie, pop_count


if __name__ == '__main__':
    RatingsPopularity.run()
