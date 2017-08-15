
from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MRMax(MRJob):

#	def step(self):
#		return [
#			MRStep(mapper=self.mapper1,
#			       reducer=self.reducer1),
#			MRStep(reducer=self.reducer2)
#		]

	def mapper(self, _, line):
		lineJSON = json.loads(line)
		yield 'value',lineJSON["var"]

	def reducer(self, key, value):
		yield "max", max(value)

	
if __name__ == '__main__':
	MRMax.run()


		
