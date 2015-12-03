import re
inn = open('testtuits.csv','r')
#aut = open('tuits_final.csv','w')
aut8 = open('tuits_final8.csv','w',encoding='utf8')
lain = inn.readline() 
lain = inn.readline() 
niu=''
while (lain != '' ):
	lain=re.sub('\n','',lain)
	#niu=lain
	if (lain.endswith(',')):
		print("finish: "+str(lain.encode('utf-8')))
		if(niu ==''):
			aut8.write(lain+'\n')
		else:
			aut8.write(niu+'\n')
		niu=''
	else:
		print(lain.encode('utf-8'))
		niu+=' '+lain
	lain = inn.readline() 
print("All folks")
inn.close()
aut8.close()
#7",\n
