from P2 import *

from pyspark import SparkContext
import numpy as np
sc = SparkContext()


#image = sc.parallelize([[i,j] for i in xrange(2000) for j in xrange(2000)],100)
i = sc.parallelize(xrange(2000), 10)
j = sc.parallelize(xrange(2000), 10)
image = i.cartesian(j)

mandel_image = image.map(lambda x: ((x[0], x[1]),mandelbrot(x[1]/500.-2,x[0]/500.-2)))

#mandel = draw_image(mandel_image)


bins = np.linspace(1,100,100)
hist1 = plt.bar(bins,sum_values_for_partitions(mandel_image).collect())
plt.xlim([0,100])
#plt.yscale('log')
plt.savefig('p2a_hist.png')









