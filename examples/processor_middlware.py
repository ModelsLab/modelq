from modelq.middleware import Processor_middlware
from modelq import ModelQ

mq = ModelQ()


class Preprocessor(Processor_middlware):
    def start(self,param):
        ## prprocess stuff before task start processings 

class PostProcessor(Processor_middlware):
    def start(self , param):
        ## process stuff aftre task execution passed

mq.task(preprocessor = Preprocessor, postprocessor = PostProcessor)
def add(param):

    init_param = get_preprocessor_result()

    out_process = send_postprocessor(post_process_params)
    return param["a"] + param["b"]