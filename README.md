# Hadoop Letter Count (MapReduce)

This program is just a little program to count letters of a text with Hadoop framework.
It's my first try of development of a MapReduce feature so be cool, it's not perfect ;)

##

## How it works ?
The goal of this program is to count the number of occurrences of all letters inside a brut text file on HDFS then order them by decreased occurrences.
So, there are two MapReduce jobs :
* First one, to count the number of occurrences
* Second one, to order by decreased occurrences

## How to build it ?
***(This program has been developed thanks to IntelliJ IDEA ;))***

1. First step is to build it and generates classes files.
2. Then execute following command inside target/classes folder `jar cfve LetterCOunt.jar Main * `
3. Open an Hadoop environment then execute jar like this `hadoop jar LetterCount.jar /user/input /user/outputCount /user/outputSort`

**Remark:** this jar takes 3 parameters :
* 0 : input path which will contain the text to analyse
* 1 : output path which will contain the result of the COUNT map/reduce
* 2 : output path which will contain the result of the SORT map/reduce
