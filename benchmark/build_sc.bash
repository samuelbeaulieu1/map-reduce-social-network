#!/bin/bash

cp map-reduce-social-network/app/SocialNetwork.java .

hadoop/bin/hadoop com.sun.tools.javac.Main SocialNetwork.java
jar cf sn.jar SocialNetwork*.class

rm -f SocialNetwork.java
rm -f SocialNetwork*.class