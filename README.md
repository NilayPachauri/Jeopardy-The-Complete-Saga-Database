# Jeopardy! The Complete Saga Database

## Description
Create a database for the iOS App "Jeopardy! The Complete Saga".


## Content
The python script `jarchive_scraper.py` is a scraper which collects all the clues from online database of Jeopardy! questions located at `j-archive.com`. It connects to a Firebase instance and populates that instance with a single collection known as `clues`. The fields of every document (uniquely identified by the custom `uid`) in the `clues` collection are as follows:
  - Air Date
  - Episode
  - Category
  - Dollar Value
  - Question
  - Answer
