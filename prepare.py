from gevent import monkey; monkey.patch_socket()
import gevent
from gevent.pool import Pool
import json
import urllib2
import sys
import random
import itertools
import math
import time

pool = Pool(4)

starttime = time.time()
reqcount = 0

def get_json(url):
  retry = 2
  while True:
    try:
      if url.startswith('/'):
        url = url[1:]
      print "trying %s" % url,
      cachekey = 'cache/%s' % url.encode('hex')
      try:
        data = file(cachekey).read()
      except:
        global reqcount
        reqcount += 0.7
        currenttime = time.time()
        wait = reqcount - (currenttime - starttime)
        if wait > 0:
          gevent.sleep(wait)
        data = urllib2.urlopen('http://oknesset.org/api/v2/'+url).read()
        file(cachekey,'w').write(data)
      data = json.loads(data)
      print "OK"
      return data
    except Exception, e:
      print "ERROR %s" % e
      gevent.sleep(random.randint(retry,retry*2))
      #retry = retry * 2

def get_all_members():
  return get_json('member/')['objects']

def get_single_member(member):
  return get_json('member/%s/' % member['id'])

def get_bill_bunch(offset):
  return get_json('bill/?limit=100&offset=%d' % offset)['objects']

def get_simple_bill_info():
  bills = get_json("bill/?limit=1")
  bill_count = bills['meta']['total_count']
  return pool.imap(get_bill_bunch,range(0,bill_count,100))

def get_full_bill(bill):
  return get_json(bill['absolute_url'])

def process_links(proposer_list,reduced_member_info):
  link_map = {}
  member_ids = set(reduced_member_info.keys())
  for proposition in proposer_list:
    prop_len = len(proposition)
    if prop_len < 2:
      continue
    prop_score = 2.0/(prop_len*(prop_len-1))
    for x in proposition:
      for y in proposition:
        if y <= x:
          continue
        if y in member_ids and x in member_ids:
          key = json.dumps([x,y])
          link_map.setdefault(key,{'value':0,'count':0,'count2':0})
          link_map[key]['value'] += prop_score
          link_map[key]['count'] += 1
          if prop_len == 2:
            link_map[key]['count2'] += 1
  values = [ x['value'] for x in link_map.values() ]
  mean = sum(values) / len(values)
  std = math.sqrt(sum((v - mean)*(v - mean) for v in values) / len(values))

  links = []
  for key,rec in link_map.iteritems():
    source,target = json.loads(key)
    value = rec['value']
    if value > mean:
      percentile = math.erfc((value-mean)/std/math.sqrt(2))*100
      for x in [source,target]:
        reduced_member_info[x].setdefault('percentile',100)
        reduced_member_info[x]['percentile'] = min(percentile,reduced_member_info[x]['percentile'])
      links.append({'source':source,
                    'target':target,
                    'value':value,
                    'count':rec['count'],
                    'count2':rec['count2'],
                    'percentile': percentile
                   })
  return links


if __name__=="__main__":
  members = get_all_members()
  members = ( member for member in members if member['is_current'] )

  reduced_member_info=[]
  full_member_info = pool.imap(get_single_member,members)
  for info in full_member_info:
      reduced_member_info.append( {
        'id': info['id'],
        'party':int(info['party_url'].split('/')[-2]),
        'name':"%(name)s" % info,
        'fullname':"%(name)s - %(party_name)s" % info
      } )
  reduced_member_info = dict((x['id'],x) for x in reduced_member_info)

  simple_bill_info = get_simple_bill_info()
  full_bill_info = pool.imap(get_full_bill,itertools.chain.from_iterable(simple_bill_info))
  full_bill_info = (bill for bill in full_bill_info if int(bill['stage_date'].split('-')[0])>=2013)

  proposer_list = ([ int(x.split('/')[-2]) for x in bill['proposers'] ] for bill in full_bill_info)
  links = process_links(proposer_list,reduced_member_info)
  nodes = reduced_member_info

  out = file('data.js','w')
  out.write("""
nodes = %s;
links = %s;
  """ % (
  json.dumps(nodes),
  json.dumps(links)
  ))
