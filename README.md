# OddValueIdentifier

Given a sequence of key-value pairs, identify, for each key, which number of values occur an odd number of times.

How to use locally:

For Windows:
* Download hadoop and add it to Environment Variables as described here:
https://stackoverflow.com/questions/41851066/exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io

In IntelliJ:
* Click 'Edit Configurations'
* Click 'Modify Options'
* Click 'Add VM Options'
* Add the following '--add-exports java.base/sun.nio.ch=ALL-UNNAMED'
