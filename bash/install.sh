#Add JDK for Ubuntu
sudo add-apt-repository ppa:webupd8team/java -y
sudo apt-get update && sudo apt-get dist-upgrade -y && sudo apt-get autoremove -y
#Install JDK 8 for Ubuntu
sudo apt-get install oracle-java8-installer -y
#Download Spark 1.5.1
wget http://www.us.apache.org/dist/spark/spark-1.5.1/spark-1.5.1-bin-hadoop2.6.tgz
#Unpack Spark
tar xf spark*
sudo apt-get install pip3
sudo pip3 install numpy
sudo pip3 install nltk
sudo pip3 install ipython

#para el master
#en /etc/host agregar ip <--> nombre 
#en set-env agregar el host o la ip
#desde el slave
#agregar hosntame <-->ip al /etc/hosts
#sbin/start-slave.sh spark://<hostname>:<port> -m 8g
