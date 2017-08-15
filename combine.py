
from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class Combine(MRJob):

#	def step(self):
#		return [
#			MRStep(mapper=self.mapper1,
#			       reducer=self.reducer1),
#			MRStep(reducer=self.reducer2)
#		]

	def mapper(self, _, line):
		lineJSON = json.loads(line)
		cord = (lineJSON["word"]["_1"], lineJSON["word"]["_2"])
		yield cord ,lineJSON["var"]

	def reducer(self, key, value):
		yield key, max(value)

	
if __name__ == '__main__':
	Combine.run()


		
