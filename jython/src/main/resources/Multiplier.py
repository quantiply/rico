class Multiplier:

  def multiply(self, x, y):
    return x * y

x = Multiplier().multiply(5, 7)


import sys
#print sys
import os

from foo.bar import Baz

zzz = Baz()
#print(zzz.buzz("grrr"))

#print(os.getcwd())

def f(a):
  return a +2

print(f(200))

from com.quantiply.api import Envelope
#from com.qply import Orange

def process(oranges):
  #print(",".join([str(orange) for orange in oranges]))

  map = oranges[3].payload
  #print([type(a[1]) for a in map.iteritems()])
  e = Envelope()
  e.headers = {"hi" : "dsfsdf", 1:2}
  e.payload={"boom": len(oranges)}
  return [e, oranges[0], oranges[1]]
