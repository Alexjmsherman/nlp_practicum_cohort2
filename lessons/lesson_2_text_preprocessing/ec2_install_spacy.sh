#!/bin/bash
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
sudo yum update -y
sudo yum groupinstall 'Development Tools' -y
sudo easy_install pip
sudo yum install python-devel -y
sudo pip install gensim pandas
sudo pip install spacy
sudo python -m spacy download en_core_web_lg