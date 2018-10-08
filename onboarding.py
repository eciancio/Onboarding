import mysql.connector
import requests
from bs4 import BeautifulSoup


def get_combine_data(cursor, cnx):
    ''' This will get combine data from http://nflcombineresults.com/nflcombinedata.php 
    and place that data into a table named WSA.nfl on the local host
    Takes a mysql connector cursor and connection object as parameters'''

    url = "http://nflcombineresults.com/nflcombinedata.php" 
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    rows = soup.find_all("tr")
    stmt = "Insert into nfl (year, playerName, college, position, height, weight, dash, bench, leap, broad, shuttle, cone) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    for row in rows[1:]:
        try:
            datum = row.find_all("td") # get all the datum and apply to functions
            year = datum[0].text
            name = datum[1].text
            college = datum[2].text
            pos = datum[3].text
            height = datum[4].text
            weight = datum[5].text
            wonderlic = datum[6].text
            fyd = datum[7].text
            bench = datum[8].text
            leap = datum[9].text
            jump = datum[10].text
            shuttle = datum[11].text
            cone = datum[12].text

            # place all the datum into a tuple
            inserts = (year, name, college, pos, height, weight, fyd, bench, leap, jump, shuttle, cone)
            inserts = clean_tuple(inserts) # clean the tuple
           
        except IndexError:
            pass # If you went past the end
        
        cursor.execute(stmt, inserts) # execute the sql statement
        cnx.commit()


    return None

def main():
    ''' Main function to create mysql connector object run get_combine_data '''
    cnx = mysql.connector.connect(user="root",
            host="127.0.0.1",
            database="WSA",
            password="")                                                                                                               
    cursor = cnx.cursor()

    clear_table(cursor, cnx)
    get_combine_data(cursor, cnx)

    # testing section don't edit
    try:
        testing_data(cursor, cnx)
    except Exception as e:
        print "Testing failed with error:" 
        print e

def clean_tuple(inserts):
     inserts = list(inserts)
     for i in range(len(inserts)):
         if not inserts[i] or inserts[i] == u'9.99':
            inserts[i] = None
     return tuple(inserts)

def testing_data(cursor, cnx):
    ''' This is just a quick test to see if the first row of the database is correct '''

    select = "Select * from NFL where playerID = 1"
    cursor.execute(select)
    response = cursor.fetchall()[0]
    name = response[2]
    bench = response[8]

    if name == "Josh Adams" and bench == 18 :
        print "Testing Passed"
    else:
        print "First Row Does Not Match"

def clear_table(cursor, cnx):
    ''' Helper Function to call in order to clear table after mistakes '''

    cursor.execute("Delete from NFL")
    cursor.execute("ALTER TABLE NFL AUTO_INCREMENT = 1")
    cnx.commit()

if __name__=="__main__":
    main()
