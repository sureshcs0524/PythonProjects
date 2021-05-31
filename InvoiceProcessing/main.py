from InvoiceProcess import InvoiceProcess
from flask import Flask, jsonify, render_template, request, redirect
from markupsafe import escape
import json
import pandas as pd
import predicate as predicate

app = Flask(__name__)

def print_hi(name):
    print(f'Program for - {name}')

# Using Python Flask API
# Home Page
@app.route('/')
def Introduction():
    return render_template('content.html')

# Index Page - To display all the list of operations to perfom on Invoice Matching Appication
@app.route('/Index')
def Index():
    return render_template('index.html')


#All records Page - To display all the list of records loaded from json file created by fetch_all method
@app.route('/All', methods=("POST", "GET"))
def Allrecords():
    if request.method == 'POST':
        return redirect('/Index')
    data = pd.read_json(r'/Users/sureshcs/PycharmProjects/output/allrecords.json',
                        dtype=['str', 'str', 'str', 'str', 'str', 'float', 'float', 'str',
                               'str', 'str', 'str'])
    df = pd.DataFrame(data, columns=['ID', 'INVOICE_NO', 'DATE', 'TYPE', 'CONTRACT_NO', 'DEBIT', 'CREDIT', 'DUE_DATE',
                                     'DOCUMENT', 'LETTERING', 'RECOVERY'])
    return render_template('AllRecords.html', tables=[df.to_html(classes='data')], titles=df.columns.values)

#Matched Invoices Page - To display Matched Invoices & Payment list of records from json file created from the fetch_matchedInvoices method
@app.route('/MatchedInvoices', methods=("POST", "GET"))
def MatchedInvoices():
    if request.method == 'POST':
        return redirect('/Index')
    data = pd.read_json(r'/Users/sureshcs/PycharmProjects/output/MatchedRecords.json', dtype=['str', 'float', 'float'])
    df = pd.DataFrame(data, columns=['DEBIT', 'CREDIT']).reset_index()
    df.columns = ['LETTERING', 'DEBIT', 'CREDIT']
    return render_template('MatchedInvoicesPayment.html', tables=[df.to_html(classes='data')], titles=df.columns.values)

#Unmatched Invoices Page - To display UnMatched Invoices list of records from json file created from the fetch_unmatchedInvocies method
@app.route('/UnmatchedInvoices', methods=("POST", "GET"))
def UnmatchedInvoices():
    if request.method == 'POST':
        return redirect('/Index')
    df = pd.read_json(r'/Users/sureshcs/PycharmProjects/output/UnMatchedInvcRecords.json',
                      dtype=['str', 'str', 'float', 'float'])
    df.columns = ['INVOICE_NO', 'RECOVERY', 'DEBIT', 'CREDIT']
    # df = pd.DataFrame(data, columns=['INVOICE_NO'])
    return render_template('UnmatchedInvoices.html', tables=[df.to_html(classes='data')], titles=df.columns.values)

#Unmatched Summary Page - To display UnMatched Summary list of records from json file created from the fetch_unmatchedSummary method
@app.route('/UnmatchedSummary', methods=("POST", "GET"))
def UnmatchedSummary():
    if request.method == 'POST':
        return redirect('/Index')
    df = pd.read_json(r'/Users/sureshcs/PycharmProjects/output/UnMatchedSumRecords.json',
                      dtype=['float', 'float']).reset_index()
    df.columns = ['RECOVERY', 'DEBIT', 'CREDIT']
    return render_template('UnmatchedSummary.html', tables=[df.to_html(classes='data')], titles=df.columns.values)


if __name__ == '__main__':
    print_hi('Invoice and Payment Matching Applicaiton')

    #Invoice Matching Application - Object creation and execution of desired methods.
    p1 = InvoiceProcess()
    p1.readInfiles()
    p1.fetch_all()
    p1.fetch_matchedInvoices()
    p1.fetch_unMatchedInvoices()
    p1.fetch_unMatchedSummary()

    #Python Flask API app execution
    app.run(debug=True)