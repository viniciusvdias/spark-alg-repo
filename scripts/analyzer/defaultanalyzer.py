import sys
from analyzer import Analyzer

def main(logfile):
   analyzer = Analyzer (logfile)

   all_actions = analyzer.run()

   params = []

   for adpt_point,actions in all_actions.iteritems():
      actionsstr = [str(a) for a in actions]
      params.append (";".join(actionsstr))

   #print ("\n:: Add the following parameter (opt plan) to your application ::")
   print ("%s" % ";".join(params))

if __name__ == "__main__":
   main(sys.argv[1])

# vim: tabstop=8 expandtab shiftwidth=3 softtabstop=3
