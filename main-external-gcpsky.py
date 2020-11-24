from flask import Flask, request, json
import datetime
from google.cloud import pubsub_v1

app = Flask(__name__)

#funzione per l'invio di dati a pubsub
def toPubSub(data):
    project_id = "sky-it-tech-itv-myaccount-dev"
    topic_id = "analytics-topic"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    # When you publish a message, the client returns a future.
    da = json.dumps(data)
    future = publisher.publish(topic_path, da.encode('utf-8'))

#funzione per calcolare l'id della sessione
def sessiId(client_id):
    currentTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    client = client_id
    session = str(abs((hash(client))))
    session_id = session+'_'+currentTime
    return session_id

@app.route('/')
def index():
    return """
<form method="POST" action="/addResource" enctype="multipart/form-data"
  <label for="fname">IL SERVIZIO SUL GRUPPO DI ISTANZE SUPERVISIONATO E' ONLINE</label><br>
  <br>
</form>
"""



@app.route('/tvapp-analytics/eventFrame', methods=['POST'])
#server-istances group
def event():
    event_data = request.data
    data = json.loads(event_data)

    if "device_info" in data:
        #sessionStart
        cl_id = data["client_id"]
        event_timest = data["device_info"]["event_ts"]
        session_id = sessiId(cl_id)
        # todo svirgolettare session_id valore
        # todo gestire bene timestamp, su tm si chiama msg_ts
        messaggio = {"properties": {"event_name": "SESSION_BEGIN", "session_id": "session_id", "client_id": "client_id", "event_timestamp":data["device_info"]["msg_ts"],
                                    "device_info": data["device_info"]}, "info": {}, "opt_info": {}}
        toPubSub(messaggio)
        toClient = {"session_id": "session_id", "format": "JSONorMP"}
        b = json.dumps(toClient)
        return b
    elif "session_id" in data:
        #sessionend
        timest = data["event_timestamp"]
        risp = {"properties": {"event_name": "SESSION_END", "session_id": "session_id", "client_id": "client_id",
                     "event_timestamp": timest}, "info": {}, "opt_info": {}}
        toPubSub(risp)
        b = json.dumps(risp)
        return b
    else:
        # evtframe
        for x in data:
            x["properties"]["app_code"] = "XXCXCX"
            toPubSub(x)
        return "frame sent"

if __name__ == '__main__':
	# This is used when running locally. Gunicorn is used to run the
	# application on Google App Engine. See entrypoint in app.yaml.
	app.run(host='0.0.0.0', port=80, debug=True)
# [END gae_flex_storage_app]
