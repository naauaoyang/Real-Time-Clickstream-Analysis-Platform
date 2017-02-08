from flask import render_template, request
from flask import jsonify

from app import app

from datetime import datetime

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra
cluster = Cluster(['ec2-34-192-175-58.compute-1.amazonaws.com'])
session = cluster.connect('wiki')

@app.route('/batch')
def batch():
    return render_template("batch.html")

@app.route("/batch", methods=['POST'])
def wiki_post_source():
    task = request.form["task"]
    keywords = request.form["keywords"]
    
    if task == 'pagerank':
        stmt = "SELECT * FROM pagerank where date = '%s' LIMIT 10" % keywords 
        response = session.execute(stmt)
        response_list = []
        for val in response:
            response_list.append(val)
        jsonresponse = [{"date": x.date, "rank": x.rank, "topic": x.topic} for x in response_list]
        return render_template("pagerank.html", output=jsonresponse)
    
    elif task == 'source':
        # reformat the input source so that it can match the exact term in the database 
        if keywords == 'google' or 'wikipedia' or 'bing' or 'yahoo' or 'twitter' or 'facebook':
            keywords = 'other-' + keywords           
        stmt = "SELECT * FROM batch_source WHERE source=%s LIMIT 10"
        response = session.execute(stmt, parameters=[keywords])
        response_list = []
        for val in response:
            response_list.append(val)
        jsonresponse = [{"source": x.source, "count": x.count, "topic": x.topic} for x in response_list]
        return render_template("source.html", output=jsonresponse)
    
    elif task == 'topic':       
        stmt = "SELECT * FROM batch_topic WHERE topic=%s LIMIT 10"
        response = session.execute(stmt, parameters=[keywords])
        response_list = []
        for val in response:
            response_list.append(val)
        jsonresponse = [{"source": x.source, "count": x.count, "topic": x.topic} for x in response_list]
        return render_template("topic.html", output=jsonresponse)

    
@app.route('/realtime')
def test():
    return render_template("realtime.html")

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