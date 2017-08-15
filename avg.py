
from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MRAvg(MRJob):

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
		n = 0
		s = 0
		for i in value:
			n += 1
			s +=i
		avg = float(s)/float(n)
		yield "average", avg

	
if __name__ == '__main__':
	MRAvg.run()


		
