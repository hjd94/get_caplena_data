import caplena
import pandas

API_KEY = ''
UNIQUE_IDENTIFIER_COL = "ID" # This is used to merge the row which is across multiple projects
CATEGORIES_TO_EXCLUDE = ["OTHER", "NULL"] #Name of categories to exclude (not codes, will add this functionality later)
file_name = ".csv"
project_names = ["", ""]


def column_dic(project):
    """Create a dictionary with Caplena colum Reference as keys and Column names as values"""
    column_dic = {}
    for column in project.columns:
        column_dic.update({column.ref: column.name})
    return column_dic


def process_row(row, column_names):
    """this is used to process a project row. It excludes unnecessary categories.
        It returns a dictionary of column_name: value """

    row_dic = {}
    for column in row.columns:
        name = column_names[column.ref]
        if column.type == "text_to_analyze":
            ref_to_val = {name: column.value}
            row_dic.update(ref_to_val)
            for topic in column.topics:
                if topic.category not in CATEGORIES_TO_EXCLUDE:
                    ref_to_val_topic = {f"{name}: {topic.category}: {topic.label}": 1}
                    row_dic.update(ref_to_val_topic)
        else:
            ref_to_val = {name: column.value}
            row_dic.update(ref_to_val)
    return row_dic


for project_name in project_names:
    rows_list = []
    client = caplena.Client(api_key=API_KEY)
    project = client.projects.retrieve(id=project_name)
    column_names = column_dic(project)
    rows = project.list_rows()
    for row in rows:
        rows_list.append(process_row(row, column_names))
    if 'df' not in locals():
        df = pandas.DataFrame(rows_list)
    else:
        temp_df = pandas.DataFrame(rows_list)
        df = df.merge(temp_df, on=UNIQUE_IDENTIFIER_COL, how='left')

df.to_csv(file_name, index=False, encoding="utf-8")
