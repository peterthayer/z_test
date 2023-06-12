import sys
import csv
import datetime
import pandas as pd

from pandas.core.groupby.groupby import DataError

CUSTOMER_FILE = '.\\data\\customer1.txt'
INVOICE_FILE = '.\\data\\invoice1.txt'
ITEM_FILE = '.\\data\\item1.txt'

# If load size in memory is a concern (which it wouldn't be if the join is done on the db) you can modify read_csv
# to use chunksize and iterate over the chunks.  I didn't do this. My assumption was task is to efficiently manipulate 
# the non-normal data and parse out the “ delimiters.  I did try to be efficient by performing the joins on each stage 
# and not keeping full dataframes in RAM.

# The datafile is also a bit ~ and the item totals don't add up to the invoice totals.  But, I assumed this wasn't of
# interest to address.

def load_customer(fname, lname):
    try:
        df = pd.read_csv(CUSTOMER_FILE, sep=',', header=0, names=["custid", "fname", "lname"],
                        quotechar='”', engine='python')
        m1 = df.loc[(df['lname'] == lname) & (df['fname'] == fname)]

    except DataError:
        print(f"Error loading {CUSTOMER_FILE}")
        m1 = None

    return m1

def join_invoices(cdf):
    try:
        df = pd.read_csv(INVOICE_FILE, sep=',', header=0, names=["custid", "invoice_id", "amount", "date"],
                    dtype = {'custid': str, 'invoice_id': str, 'amount': float}, #parse_dates= ['date'], #Some dates are wrong eg 0-Jan
                    quotechar='”', engine='python')
        jdf = cdf.set_index('custid').join(df.set_index('custid'), how = 'inner')
    except DataError:
        print(f"Error loading {INVOICE_FILE}")
        jdf = None

    return jdf


def join_items(jdf):
    try:
        df = pd.read_csv(ITEM_FILE, sep=',', header=0, names=["invoice_id", "item_id", "item_amount", "quantity"],
                    dtype = {'invoice_id': str, 'item_id': str, 'item_amount': float, 'quantity': int}, 
                    quotechar='”', engine='python')
        df.iloc[:,0] = df.iloc[:,0].str.replace('”', '')
        df.iloc[:,0] = df.iloc[:,0].str.replace('“', '')

        tdf = jdf.join(df.set_index('invoice_id'), on='invoice_id', how = 'left')

    except DataError:
        print(f"Error loading {ITEM_FILE}")
        jdf = None

    return tdf

def print_history(fname, lname, df):
    customerId = ''
    invoiceId = ''

# Can't workably sort as some date values aren't dates e.g. 0-Jan-2010
#   df = df.sort_values(by =['custid','date'], ascending = [False,True])

    for index, row in df.iterrows():
        cid = index.replace('“', '').replace('”', '')
        if ( cid != customerId ):
            print(f'\nPurchase history for Customer ID:{cid} Name: {sys.argv[1]} {sys.argv[2]}')
            customerId = cid

        iid = row['invoice_id']

        if ( iid != invoiceId ):
            invoiceId = iid
            invoiceAmt = '${:,.2f}'.format(row['amount'])
            invoiceDate = row['date']
            print(f'{invoiceDate} Invoice {invoiceId} Total {invoiceAmt}')
            print('\tItem #\tUnit Price\tQty')

        
        itemId = row['item_id']
        itemPrice = '${:,.2f}'.format(row['item_amount'])
        itemQty = row['quantity']
        print(f'\t{itemId}\t {itemPrice}\t\t {itemQty}')


if __name__ == "__main__":

    if ( len(sys.argv) != 3 ):
        print(f'Application requires customer name.  Arg count is:{len(sys.argv)}')
    else:
        cdf = load_customer(sys.argv[1], sys.argv[2])
        if ( cdf is not None and cdf.shape[0] > 0 ):
            print(f'{cdf.shape[0]} customer IDs matching {sys.argv[1]} {sys.argv[2]}')
            jdf = join_invoices(cdf)

            if ( jdf is None or jdf.empty == True ):
                print('There are no matching invoices to the customer ID(s)')             
            else:
                tdf = join_items(jdf)
                if ( tdf is not None ):
                    print_history(sys.argv[1], sys.argv[2], tdf)
        else:
            print(f'No matching customers records found for {sys.argv[1]} {sys.argv[2]}')




