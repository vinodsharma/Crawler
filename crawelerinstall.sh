mkdir crawlerinstall
cd crawlerinstall
apt-get -y install git
apt-get -y install python-dev python-ply
apt-get -y install build-essential
#apt-get build-dep libwebkit-1.0-2
#apt-get install libwebkit-1.0-2
# next line will do work of last two lines
apt-get -y build-dep libwebkit-dev
apt-get -y install libwebkit-dev
aptitude -y install gtk-doc-tools
apt-get -y install bison
#install pycario
wget http://cairographics.org/releases/pycairo-1.8.8.tar.gz
tar xvf pycairo-1.8.8.tar.gz
cd pycairo-1.8.8/
./configure 
make
make install
cd ../
#install pygonjects
wget http://ftp.gnome.org/pub/GNOME/sources/pygobject/2.21/pygobject-2.21.3.tar.gz
tar xvf pygobject-2.21.3.tar.gz
cd pygobject-2.21.3/
./configure 
make
make install
cd ../
#install pygtk
git clone git://git.gnome.org/pygtk
cd pygtk
./autogen.sh
make
make install
cd ../

#install rabbitmq server
apt-get -y install rabbitmq-server
apt-get -y install python-pip
pip install pika
pip install xlrd
pip install xlwt

#install couchdb
wget http://packages.couchbase.com/releases/1.7.0/membase-server-community_x86_64_1.7.0.deb
dpkg -i membase-server-community_x86_64_1.7.0.deb
wget http://peak.telecommunity.com/dist/ez_setup.py
python ez_setup.py
wget http://pypi.python.org/packages/2.6/C/CouchDB/CouchDB-0.8-py2.6.egg
easy_install CouchDB-0.8-py2.6.egg

#install python webkit
#git clone git://git.savannah.gnu.org/pythonwebkit.git
#cd pythonwebkit
#git checkout python_codegen
#./autogen.sh
#make
#make install
#cd ../
