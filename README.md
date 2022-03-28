# Ethereum-Blockchain-Analysis

Task 1 -

---How to run---
-> Install the Python Pandas library if it is not installed. 
  **pip install pandas**
  
-> Run the python code.
  **python group8_assign1.py**
  
-> This file first asks which directory the user would like to pull data from. Please enter the path of a directory.

-> Next, the program asks the user which query they would like to run. Here all queries can be run in sequence by entering ‘all’, or individual queries can be run by entering their number from “1 to 10”.

-> Queries 1 through 5 require no input from the user and run independently. Queries 6-10 will ask the user for an input to match a category to. These can be input before the query is run when prompted by the program. However, by entering nothing or waiting a short time (“20 sec”) the query will be run with a default value.

-> After each query is run the program will output its time taken to read from the file, process the data needed for the query, and sort/search the data for display.

-> The result will be written in a .csv file for the top 100 records. The file will be saved in the “/outputs” folder.
