import sqlite3

conn = sqlite3.connect('spider2.sqlite')
cur = conn.cursor()

# Find the ids that send out page rank - we only are interested
# in pages in the SCC that have in and out links
cur.execute('''SELECT DISTINCT from_id FROM Links''')
from_ids = list()
for row in cur: 
    from_ids.append(row[0])

# Find the ids that receive page rank 
to_ids = list()
links = list()
cur.execute('''SELECT DISTINCT from_id, to_id FROM Links''')
for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id : continue
    #the urls that are not in links are skipped
    if from_id not in from_ids : continue 
    #to_id that points to nowhere/not yet retrives yet are skipped
    if to_id not in from_ids : continue 
    #links that are already retrives and have a valid from_id and to_id are selected
    links.append(row)
    #list of to_id's
    if to_id not in to_ids : to_ids.append(to_id)

# Get latest page ranks for strongly connected component
# Get dictionary of all from_id's with their latest page rank from Pages table i.e mapping id to its rank
prev_ranks = dict()
for node in from_ids:
    cur.execute('''SELECT new_rank FROM Pages WHERE id = ?''', (node, ))
    row = cur.fetchone()
    prev_ranks[node] = row[0]

sval = input('How many iterations:')
many = 1
if ( len(sval) > 0 ) : many = int(sval)

# Sanity check
if len(prev_ranks) < 1 : 
    print("Nothing to page rank.  Check data.")
    quit()

# Lets do Page Rank in memory so it is really fast
for i in range(many):
    # print prev_ranks.items()[:5]
    # create new dict for next_ranks
    next_ranks = dict()
    total = 0.0
    for (node, old_rank) in list(prev_ranks.items()):
        total = total + old_rank
        next_ranks[node] = 0.0
    # print total

    # Find the number of outbound links and sent the page rank down each
    for (node, old_rank) in list(prev_ranks.items()):
        # print node, old_rank
        give_ids = list()
        for (from_id, to_id) in links:
            if from_id != node : continue
           #  print '   ',from_id,to_id

            if to_id not in to_ids: continue
            # inbound link from node to to_id, so consider that in bound link and accumulate that inbound link
            give_ids.append(to_id)
        if ( len(give_ids) < 1 ) : continue
        # calculate ranks
        amount = old_rank / len(give_ids)
        # print node, old_rank,amount, give_ids
    
        # assign calculated ranks to all the pages that are
        for id in give_ids:
            next_ranks[id] = next_ranks[id] + amount
    
    # Now calculate new total PageRank
    newtot = 0
    for (node, next_rank) in list(next_ranks.items()):
        newtot = newtot + next_rank
    # There are dysfunctional shapes in which PageRank can be trapped.
    # This EVAP is taking a fraction away from everyone and giving it back to everybody else
    # Evaporation has somethinf to do with PageRank not +sure.
    evap = (total - newtot) / len(next_ranks)

    # print newtot, evap
    for node in next_ranks:
        next_ranks[node] = next_ranks[node] + evap

    newtot = 0
    for (node, next_rank) in list(next_ranks.items()):
        newtot = newtot + next_rank

    # Compute the per-page average change from old rank to new rank
    # As indication of convergence of the algorithm
    # shows the stability of the PageRank algo
    totdiff = 0
    for (node, old_rank) in list(prev_ranks.items()):
        new_rank = next_ranks[node]
        diff = abs(old_rank-new_rank)
        totdiff = totdiff + diff

    avediff = totdiff / len(prev_ranks)
    print(i+1, avediff)

    # rotate
    prev_ranks = next_ranks

# Put the final ranks back into the database
print(list(next_ranks.items())[:5])
cur.execute('''UPDATE Pages SET old_rank=new_rank''')
for (id, new_rank) in list(next_ranks.items()) :
    cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (new_rank, id))
conn.commit()
cur.close()

