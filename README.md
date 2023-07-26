# ng-apidbmigration
This repository contains scripts to migrate data from intermediate data sources to api databases of member, membership services. This repository would also contain scripts to create tags in Elastic search and to resgiter members in KeyCloak using their Restful APIs.

## Installation Environment

Python version to be used: 

`3.7.7`

Installing the dependencies: 

`pip install -r requirements.txt , or pip3 install -r requirements.txt`

Running Scripts:

`python loadmember.py`

`python loadmembership.py`

`python tags.py`


Before running scripts please ensure that database.ini and properties.ini has correct values as per environments in which you are running the scripts. Datbase connectivity passwords as masked in ini files and sql scripts. You need to add those before runnning the scripts. Please also ensure that you do not check in the passwords in the code repository.

Details of running the scripts, folder structure etc. can be found in project wiki
