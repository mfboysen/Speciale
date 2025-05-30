# Speciale

These are the files used in relation to the Master's thesis made by Frederik Boysen at Aarhus University. 

The files are numerated and a short explanation of each file will be explained here:

1. ArcticShiftData.py
The file is used to fetch reddit data through the API ArcticShift. The output is 2 csv files, where 1 is for the submissions, and another for comments.

2. GetFinanceData.py
This file extract the finance data used in the thesis.

3. MakeDataFile.py
This file merge the Reddit data and the finance data together into a final dataset with all 92 tickers for every open day within the period.

4. RedditEDA.ipynb
Here the EDA is made.

5. ML-models.ipynb
Here 3 of the ML models are made, that is logistic regression, random forest and xgboosting.

6. finbert_accuracy.ipynb
In this file the accuracy of the different models are tested to see the accuracy score.

7. LSTM.ipynb
This is where the LSTM model is trained and used to predict. 
