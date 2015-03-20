
#-*- coding:utf-8 -*-
import urllib
import urllib2
import re

pattern = re.compile(r'<li[\s\S]+?>#(\d+)<[\s\S]+?img src="(http://.+?)"[\s\S]+?</li>')
url_base = 'http://jandan.net/ooxx/page-%s#comments'
img_set = set()

for p in range(900,1145):
	url = url_base%p
	data = urllib2.urlopen(url).read()
	img_list = pattern.findall(data)
	print(p,len(img_list))
	img_set1 = set(img_list)
	img_set = img_set.union(img_set1)
	if len(img_list)<25:break

print('共有%s张图片'%len(img_set))

for s in img_set:
	try:
		url = s[1]
		gs = s[1].rsplit('.',1)[1]
		path_1 = '/home/luo/%s.%s'%(s[0],gs)
		urllib.urlretrieve(url,path_1)
		print(path_1,'get !')
	except:
		print(s)
print('下载完成')
