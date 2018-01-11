"{0:04b}".format()


// Autre chose

import pstats
p = pstats.Stats('profile.txt')
p.sort_stats('cumulative').print_stats(10)
