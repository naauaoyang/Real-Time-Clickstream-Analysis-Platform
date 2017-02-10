from __future__ import print_function
import sys
from datetime import datetime
from flask import render_template, request
from flask import jsonify
from app import app
from cassandra.cluster import Cluster

if len(sys.argv) != 2:
    print("Usage: tornadoapp <public_dns>", file=sys.stderr)
    exit(-1)
    
# Setting up connections to cassandra
cluster = Cluster([sys.argv[1]])
session = cluster.connect('wiki')

@app.route('/')
def batch():
    return render_template("batch.html")

@app.route("/", methods=['POST'])
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
    stmt = "SELECT * FROM realtime WHERE source=%s limit 100"
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