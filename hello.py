import os
from _datetime import datetime
import pymysql
from flask import Flask, render_template, request, url_for
from werkzeug.utils import redirect
import database
import overall_details
import sql_query_details


app = Flask(__name__)

db = None
result = []
ip = ""
username = ""
password = ""

database_name = ""
sql_query = ""
sql_output = ""
natural_lang_query = ""
query_type=""
# overall_detail = ""


@app.route('/login')
def main_prog():
    return render_template('login.html')


@app.route('/loginDetails',methods=['POST'])
def loginDetails():
    global username, password, ip, db
    ip = request.form['IP']
    username = request.form['username']
    password = request.form['password']
    try:
        db = pymysql.connect(ip, username, password)
        if (db):
            global result
            result = execute_query(db, "SHOW DATABASES")
            result.remove("information_schema")
            result.remove("performance_schema")
            result.remove("mysql")
            result.remove("sys")
            # print(username, password)
            rend_request = redirect(url_for('loggedin'))
            db.close()
        else:
            print("Invalid username or password")
            rend_request = render_template('login.html',invalid= True)
    except Exception as e:
        print(str(e))
        print("Invalid username or password")
        rend_request = render_template('login.html',invalid= True)
            # render(request, 'login.html', {'invalid': True})
    return rend_request


@app.route('/loggedin')
def loggedin():
    global result, username, ip
    # print("Result : ",result)
    return render_template('loggedin.html', db=False,result=result, username=username, ip=ip)


@app.route('/selected_db', methods=['POST'])
def selected_db():
    global database_name, ip
    database_name = request.form['myDropdown']
    # print("Database : ",database)
    return render_template('loggedin.html', ip=ip,database=database_name, db=True, generate=False, exec_flag=False,result=result, username=username,nlq="")


@app.route('/handle_option', methods=['POST'])
def handle_option():
    opt_type = False
    opt_type2 = False
    print("handle options")
    if request.form['opt_button'] == "text_bt":
        opt_type = True
    elif request.form["opt_button"] == "file_bt":
        opt_type2 = True
    return render_template('loggedin.html', ip=ip, opt_type=opt_type,opt_type2=opt_type2,result=result,username=username,database=database_name, db=True)


@app.route('/query_process', methods=['POST'])
def query_process():
    error_flag = 0
    error_string = ""
    column_names = ""
    print("query process1:", request.form['query_button'])
    global database_name, sql_query, sql_output, username, password, ip, db, natural_lang_query, query_type
    db = database.Database(ip, username, password, database_name)
    db.connect()
    try:
        overall_detail = overall_details.OverallDetails(db)
        overall_detail.collect_details()
        sql_query_details_obj = sql_query_details.SQLQueryDetails(db, overall_detail)
        natural_lang_query = request.form['input']
        print("query process:", request.form['query_button'])
        if request.form['query_button'] == "gener_bt":
            clauses = sql_query_details_obj.collect_query_details(natural_lang_query)
            [sql_query, query_type] = clauses.create_query()
            if query_type == "S":
                try:
                    db.execute_query(sql_query)
                except Exception:
                    error_flag = 1
                    error_string = "System failed to generate SQL query for the given input."

            if error_flag == 0:
                write_log_file()
                return render_template('loggedin.html', ip=ip,nlq=natural_lang_query,database=database_name,db=True,
                    username=username,result=result, generate=True, sql_query=sql_query,opt_type=True,error=False)
            return render_template('loggedin.html', ip=ip, nlq=natural_lang_query, database=database_name, db=True,
                    username=username, result=result, generate=False, sql_query=sql_query,
                                   opt_type=True, error=True, error_string=error_string)

        elif request.form['query_button'] == "exec_bt":
            # natural_lang_query=request.form['nlq']
            if query_type == "S":
                sql_output = db.execute_query(sql_query, "1")
                query_result=sql_output[0]
                column_names=sql_output[1]
                print(sql_output[0], sql_output[1])
            else:
                sql_output = db.execute_query(sql_query)
                query_result=sql_output

            return render_template('loggedin.html', ip=ip,nlq=natural_lang_query,database=database_name,db=True,
                                   username=username, result=result,generate=True, sql_query=sql_query, exec_flag=True,
                                   query_result=query_result,column_names=column_names,opt_type=True)
    except Exception:
        error_string = "System failed to generate SQL query for the given input."
        return render_template('loggedin.html', ip=ip, nlq=natural_lang_query, database=database_name, db=True,
                               username=username, result=result, generate=False, sql_query=sql_query,
                               opt_type=True, error=True, error_string=error_string)


@app.route('/query_process_file', methods=['POST'])
def query_process_file():
    flag_open = 0
    error_string = ""
    global database_name, sql_query, sql_output, username, password, ip, db, natural_lang_query, query_type
    file_path = request.form['input_file']

    output_path = os.path.abspath(os.path.join(file_path,os.pardir))+"\output.txt"
    op_file = open(output_path, "w")

    db = database.Database(ip, username, password, database_name)
    db.connect()

    overall_detail = overall_details.OverallDetails(db)
    overall_detail.collect_details()

    count = 1
    try:
        open(file_path)
        flag_open = 1
    except FileNotFoundError:
        error_string = "File not found at the location specified."

    if flag_open == 1:
        with open(file_path) as fp:
            for line in fp:
                natural_lang_query = line.strip('\n')
                print("%d %s" % (count, natural_lang_query))

                sql_query_details_obj = sql_query_details.SQLQueryDetails(db, overall_detail)

                clauses = sql_query_details_obj.collect_query_details(natural_lang_query)

                [sql_query, query_type] = clauses.create_query()
                write_log_file()
                print("\n-----------")
                print("Final query: ", sql_query)
                print("-----------\n")
                op_file.write(sql_query + "\n")
                count += 1

        op_file.close()
        output_path = output_path.replace('\\', '/')
        return render_template('loggedin.html', ip=ip, nlq=file_path, database=database_name, db=True,
                               username=username, result=result, generate=True, sql_query=sql_query, exec_flag=False,
                               opt_type2=True,output_path=output_path, error=False)
    return render_template('loggedin.html', ip=ip, nlq=file_path, database=database_name, db=True,
                           username=username, result=error_string, generate=False, sql_query=sql_query, exec_flag=False,
                           opt_type2=True, output_path=output_path, error=True)


def write_log_file():
    file_name = username + ".log"
    time_stamp = datetime.now()
    time_stamp = time_stamp.strftime('%Y-%b-%d %H:%M:%S')
    fp = open(file_name, "a")
    fp.write("-------------------------------------------------------------------------------------------------"
             "-----------------------" + "\n")
    fp.write("Database Name : " + database_name + "\t\t\t\t\t\t" + time_stamp + "\n")
    fp.write("Input : " + natural_lang_query + "\n")
    fp.write("Query : " + sql_query + "\n")


def execute_query(db1, sql_query):
    cursor = db1.cursor()
    cursor.execute(sql_query)
    db1.commit()
    return retrieve(cursor)


def retrieve(cursor):
    result = list()
    temp_array = []
    for row in cursor:
        if len(row) == 1:
            result.append(str(row[0]))
        else:
            for i in range(0, len(row)):
                temp_array.append(str(row[i]))
            result.append(temp_array)
            temp_array = []
    return result


if __name__ == '__main__':
   app.run(debug=True)