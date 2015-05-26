from com.quantiply.samza.task import BaseTask
from org.apache.samza.system import OutgoingMessageEnvelope

class EchoTask(BaseTask):
  
    def _init(self, config, context, metric_adaptor):
        self.output = self.getSystemStream("out")
        self.registerDefaultHandler(self.handle_msg)
        
    def handle_msg(self, envelope, collector, coord):
        collector.send(OutgoingMessageEnvelope(self.output, envelope.message))
