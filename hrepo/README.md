Contents
=========

1. CreateDatabases : takes existing csv files and create sqlite databases.  
   the rest of the modules require the sqlite databases.
2. LoanSummary : reports high level summary of loans  
3. LoanDueDates : will compute outstanding loans and due dates

Usage
=====
Each of the above module is implemented through its main function.  
The LoanDueDates module will create two files on your local system when executed.

TODO
=====
Need to compute loan due dates and amount due for next due date.
