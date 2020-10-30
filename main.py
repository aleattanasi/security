
from flask import Flask, request, Response, jsonify, json
import datetime;
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

@app.route('/')
def index():
	return """
<form method="POST" action="/addResource" enctype="multipart/form-data"
  <label for="fname">IL SERVIZIO SUL GRUPPO DI ISTANZE SUPERVISIONATO E' ONLINE</label><br>
  <br>
</form>
"""

@app.route('/sessionBegin', methods=['POST'])
#server-istances group
def begin():
	event_data = request.data
	data = json.loads(event_data)
	cl_id = data["client_id"]
	timest = data["event_timestamp"]
	session_id = sessiId(cl_id)
	#todo svirgolettare session_id valore
	#todo gestire bene timestamp, su tm si chiama msg_ts
	data = {"properties":{"event_name":"SESSION_BEGIN", "session_id": "session_id", "client_id": "client_id", "event_timestamp":timest, 'source': "sorg", 'dev_family': "devF", 'dev_type': "devT",'dev_stb_sn': "dev_stb_sn",'dev_stb_id': "dev_stb_id",
                                   'dev_stb_ver': "dev_stb_ver",'dev_stb_model': "dev_stb_model",'dev_stb_name': "dev_stb_name",'dev_stb_man': "dev_stb_man",'dev_stb_as': "dev_stb_as",
                                   'dev_stb_ua': "dev_stb_ua",'dev_sc_sn': "dev_sc_sn",'conn_type': "conn_type",'conn_wifi_freq': "conn_wifi_freq",'conn_wifi_channel': "conn_wifi_channel",
                                   'conn_wifi_strength': "conn_wifi_strength",'conn_router_model': "conn_router_model",'user_external_id': "user_external_id",'user_country_code': "user_country_code",
                                   'user_region_name': "user_region_name",'user_province': "user_province",'user_city': "user_city",'user_isp_name': "user_isp_name", 'app_name': "app_name",
                                   'app_version': "app_version",'app_itv_be_version': "app_itv_be_version"},"info":{}, "opt_info":{}}
	toPubSub(data)

	#SU DATAFLKOW (UNA VOLTA CREATO L'ID MANDO L'EVENTO A P/S) salvo su firestore nuovo doc "clientID" con dentro id sessione e metadati da definire(context_info ad esempio)
	toClient = {"session_id" : session_id, "format" : "JSONorMP"}
	b=json.dumps(toClient)
	return b

#funzione per calcolare l'id della sessione
def sessiId(client_id):
	currentTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	client = client_id
	session = str(abs((hash(client))))
	session_id = session+'_'+currentTime
	return session_id


@app.route('/EventFrame', methods=['POST'])
#server-istances group
def event():
	#qui mi arriva o una lista di eventi o un evento e mando dirett a pubsub
	event_data = request.data
	data = json.loads(event_data)
	toPubSub(data)
	return data

@app.route('/sessionEnd', methods=['POST'])
#server-istances group
def end():
	event_data = request.data
	data = json.loads(event_data)
	timest = data["event_timestamp"]
	risp ={"properties":{"event_name":"SESSION_END", "session_id": "session_id", "client_id": "client_id", "event_timestamp":timest}, "info":{}, "opt_info":{}}
	toPubSub(risp)
	b=json.dumps(risp)
	return b


if __name__ == '__main__':
	# This is used when running locally. Gunicorn is used to run the
	# application on Google App Engine. See entrypoint in app.yaml.
	app.run(host='0.0.0.0', port=5000, debug=True)
# [END gae_flex_storage_app]
