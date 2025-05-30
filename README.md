# Speciale

These are the files used in relation to the Master's thesis made by Frederik Boysen at Aarhus University. 

The files are numerated and a short explanation of each file is given below:

1. **ArcticShiftData.py**  
   The file is used to fetch Reddit data through the ArcticShift API. The output is two CSV files: one for the submissions and another for the comments.

2. **GetFinanceData.py**  
   This file extracts the financial data used in the thesis.

3. **MakeDataFile.py**  
   This file merges the Reddit data and the financial data into a final dataset with all 92 tickers for every open day within the period.

4. **RedditEDA.ipynb**  
   This notebook contains the exploratory data analysis (EDA).

5. **ML-models.ipynb**  
   This notebook includes three of the ML models used: logistic regression, random forest, and XGBoost.

6. **finbert_accuracy.ipynb**  
   This file evaluates and compares the accuracy of the different models.

7. **LSTM.ipynb**  
   This is where the LSTM model is trained and used for prediction.
