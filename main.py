#import package:

from caplena.endpoints.projects_endpoint import ProjectDetail
import caplena
import pandas
import dotenv
import os
#________________________________________________________________________________________________________
#Load varaible in env file:

# if dotenv.find_dotenv():
#     dotenv.load_dotenv()
#________________________________________________________________________________________________________
#create function:

def column_dic(project: ProjectDetail) -> dict:
    """This takes a dataframe and creates is a dictionary
        of Caplena IDs and the given name of the column. caplena_ref:given_name"""
    col_dic = {}
    for column in project.columns:
        col_dic.update({column.ref: column.name})
    return col_dic
#________________________________________________________________________________________________________
#set varaibles:

API_KEY = ""
ID_COL_NAME = "ID"
PATH = ""
OUTPUT_NAME = ".csv"
PROJECTS = [""]
EXCLUDED_CATEGORIES = ["NONE"]
#________________________________________________________________________________________________________

def main():
    #extract data
    client = caplena.Client(api_key=API_KEY) #opens client
    for project in PROJECTS: #loop through all project listed
        records = []
        project = client.projects.retrieve(id=project) #get project from caplena
        column_names = column_dic(project) #create a lookup dictionary
        rows = project.list_rows() #get a list of the rows.
        for row in rows: #loop through the rows
            row_dic = {}
            for col in row.columns: #loop through columns in rows
                name = column_names[col.ref] #get the real name of the column
                row_dic.update({name: col.value})
                if col.type == "text_to_analyze": #check if it a text to analyse column
                    [row_dic.update({f"{name}: {topic.category}: {topic.label}": 1})
                     for topic in col.topics if topic.category not in EXCLUDED_CATEGORIES] #process  the codes in a text to analyse column
            records.append(row_dic) #append row dic to records list
        if 'df' not in locals():
            df = pandas.DataFrame(records) #create dataframe for records
        else:
            df = df.merge(pandas.DataFrame(records), on=ID_COL_NAME, how='left') #left join dataframe to get columns held across different projects

    df.to_csv(os.path.join(PATH, OUTPUT_NAME), index=False, encoding="utf-8") #save it into a file


if __name__ == '__main__':
    main()
