import predictionio
import argparse
import random
import pytz
import datetime

RATE_ACTIONS_DELIMITER = "::"

def import_events(client, file):
  f = open(file, 'r')
  count = 0
  print "Importing data..."
  for line in f:
    data = line.rstrip('\r\n').split(RATE_ACTIONS_DELIMITER)
    #time=datetime.datetime.fromtimestamp(int(data[2]), tz=pytz.utc).strftime('%Y-%m-%dT%H:%M:%S.000%z')
    #print time
    client.create_event(
    	event="$set",
    	entity_type="user",
    	entity_id=data[0],
	event_time=datetime.datetime.fromtimestamp(int(data[1])-300).replace(tzinfo=pytz.utc)
    )
    count += 1
  f.close()
  print "%s events are imported." % count


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for recommendation engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="files/users.csv")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)
