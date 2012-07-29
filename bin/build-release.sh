#!/bin/sh

RELEASE=`head -1 project.clj | awk '{print $3}' | sed -e 's/\"//' | sed -e 's/\"//'`

rm -rf _release
rm -rf lib/ classes/
rm *jar
rm *mesos*tgz

lein jar

mkdir _release
cp $1 _release/
cd _release
unzip *.zip
rm *zip
mv storm* storm
cd ..
cp *.jar _release/storm/lib/
cp lib/*.jar _release/storm/lib/
cp bin/storm-mesos _release/storm/bin/

mkdir _release/storm/native
cp zmqlibs/linux/* _release/storm/native/

cp storm.yaml _release/storm/conf/storm.yaml

cd _release
mv storm storm-mesos-$RELEASE
tar -czf storm-mesos-$RELEASE.tgz storm-mesos-$RELEASE
cp storm-mesos-$RELEASE.tgz ../
cd ..
rm -rf _release
