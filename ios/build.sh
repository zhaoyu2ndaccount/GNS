#!/usr/bin/env bash

rm -rf *.o ./edu ./org
~/Downloads/j2objc/dist/j2objc --build-closure -d . -sourcepath ../src:../../gig_ios/src -classpath /Users/kanantharamu/gig_ios/lib/json-smart-1.2.jar ../src/edu/umass/cs/gnsclient/client/GNSClient.java /Users/kanantharamu/gig_ios/src/sun/misc/Cleaner.java
~/Downloads/j2objc/dist/j2objcc -c -I. -I ~/json-smart-v1/json-smart/build  `find . -name "*.m"`
#rm ./libs/libgiga.a
#rm ReconfigurableAppClientAsyncTest.o JSONObject.o JSONArray.o JSONException.o JSONTokener.o
#ar -r libs/libgiga.a *.o
#rm *.o
#~/Downloads/j2objc/dist/j2objcc -ObjC -o test -g -I. -l jre_emul -l junit libs/libgiga.a libs/libjsonsmart.a edu/umass/cs/reconfiguration/ReconfigurableAppClientAsyncTest.m