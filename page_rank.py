
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol




class MRPageRank(MRJob):

    INPUT_PROTOCOL = JSONProtocol  # read the same format we write

    def configure_options(self):
        super(MRPageRank, self).configure_options()

        self.add_passthrough_option(
            '--iterations', dest='iterations', default=10, type='int',
            help='number of iterations to run')

        self.add_passthrough_option(
            '--damping-factor', dest='damping_factor', default=0.85,
            type='float',
            help='probability a web surfer will continue clicking on links')

    def send_score(self, node_id, node):
  	#v=node.get("links")
		
		yield node_id, ('node', node)
		tot_length=len(node)-1
		if tot_length>0:
			weight=(float(node[0])/(tot_length))
		else:
			weight=1	
		for dest_id in node[1:]:
			yield dest_id, ("score",  weight)

    def receive_score(self, node_id, values):
		total_score = 0
		node=[]
		check=""
		for out in values:
			if out[0]=="node":
				check="node"
				for h in out[1]:
					node.append(h)
			else:
				total_score += out[1]
				
			
		#i=typed_values[0]
		#if i == "node":
			#node=typed_values[1]
		#else:
			#total_score += 	int(typed_values[1])	
        
		d = self.options.damping_factor
		if check=="node":
			node[0] = str(1 - d + d * total_score)
			yield node_id, node


    def steps(self):
#        return ([self.mr(mapper=self.send_score)] *
#                self.options.iterations)
        return ([self.mr(mapper=self.send_score, reducer=self.receive_score)] *
                self.options.iterations)

if __name__ == '__main__':
    MRPageRank.run()
