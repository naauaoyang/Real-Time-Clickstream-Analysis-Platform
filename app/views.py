from flask import render_template, request
# jsonify creates a json representation of the response
from flask import jsonify

from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra
cluster = Cluster(['ec2-34-192-175-58.compute-1.amazonaws.com'])

#session = cluster.connect('playground')
session = cluster.connect('wiki')

'''
@app.route('/')
@app.route('/index')
def index():
    user = { 'nickname': 'Miguel' } # fake user
    mylist = [1,2,3,4]
    return render_template("index.html", title = 'Home', user = user, mylist = mylist)
'''

'''
@app.route('/api/<source>/')
def get_email(source):
    stmt = "SELECT * FROM batch_source WHERE source=%s LIMIT 10"
    response = session.execute(stmt, parameters=[source])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"source": x.source, "count": x.count, "topic": x.topic} for x in response_list]
    return jsonify(jsonresponse)
'''

'''
@app.route('/api/<email>/<date>')
def get_email(email, date):
    stmt = "SELECT * FROM email WHERE id=%s and date=%s"
    response = session.execute(stmt, parameters=[email, date])
    response_list = []
    for val in response:
        response_list.append(val)
        jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
    return jsonify(emails=jsonresponse)
'''


'''
@app.route('/email')
def email():
    return render_template("email.html")
'''

@app.route('/wiki')
def wiki():
    return render_template("wiki.html")



'''
@app.route("/email", methods=['POST'])
def email_post():
    emailid = request.form["emailid"]
    date = request.form["date"]

    #email entered is in emailid and date selected in dropdown is in date variable respectively

    stmt = "SELECT * FROM email WHERE id=%s and date=%s"
    response = session.execute(stmt, parameters=[emailid, date])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"fname": x.fname, "lname": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
    return render_template("emailop.html", output=jsonresponse)
'''


@app.route("/wiki", methods=['POST'])
def wiki_post_source():
    source_or_topic = request.form["source_or_topic"]
    keywords = request.form["keywords"]
    
    if source_or_topic == 'source': 
        # reformat the input source so that it can match the exact term in the database 
        if keywords == 'google' or 'wikipedia' or 'bing' or 'yahoo' or 'twitter' or 'facebook':
            keywords = 'other-' + keywords           
        stmt = "SELECT * FROM batch_source WHERE source=%s LIMIT 10"
    else:
        stmt = "SELECT * FROM batch_topic WHERE topic=%s LIMIT 10"
        
    response = session.execute(stmt, parameters=[keywords])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"source": x.source, "count": x.count, "topic": x.topic} for x in response_list]
    
    return render_template("sourceop.html", output=jsonresponse)

'''
@app.route('/realtime')
def realtime():
    return render_template("realtime.html")
'''

@app.route('/test')
def test():
    return render_template("test1.html")

@app.route('/hrini/<source>/')
def get_hrini(source):
    stmt = "SELECT * FROM realtime WHERE source=%s limit 10"
    response = session.execute(stmt, parameters = [source])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"source":x.source,"count":x.count,"time":x.timestamp} for x in response_list]
    return jsonify(records = jsonresponse)

@app.route('/hr/<source>/')
def get_hr(source):
    stmt = "SELECT * FROM realtime WHERE source=%s limit 1"
    response = session.execute(stmt, parameters = [source])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"source":x.source,"count":x.count,"time":x.timestamp} for x in response_list]
    return jsonify(record = jsonresponse)

@app.route('/search',methods = ['POST'])
def search_name():
    s_name = request.form["search_name"]
    if s_name in ('google', 'wikipedia', 'bing', 'yahoo', 'twitter', 'facebook'):
        jsonresponse = {"name": s_name, "check": "Name Exists"}
        return jsonify(result = jsonresponse)
    else:
        return jsonify(result={"name":s_name,"check":"Name Not Exists"})