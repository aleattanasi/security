
from flask import Flask, request, Response, jsonify, json
import datetime;
from google.cloud import pubsub_v1

app = Flask(__name__)

#funzione per l'invio di dati a pubsub
def toPubSub(data):
	project_id = "hexact"
	topic_id = "EventiSTB"

	publisher = pubsub_v1.PublisherClient()
	topic_path = publisher.topic_path(project_id, topic_id)

	# When you publish a message, the client returns a future.
	da = json.dumps(data)
	future = publisher.publish(topic_path, da.encode('utf-8'))

@app.route('/')
def index():
	return """
<form method="POST" action="/addResource" enctype="multipart/form-data"
  <label for="fname">online</label><br>
  <input type="text" id="nomeintero" name="nomeintero" value="nomeItem_nomeRisorsa">
  <input type="submit">
  <br>
</form>
"""

@app.route('/sessionBegin', methods=['POST'])
#server-istances group
def begin():
	
	event_data = request.data
	data = json.loads(event_data)
	cl_id = data["client_id"]

	#todo l'hash viene negativo a volte, se negativo lo moltiplico per -1?
	session_id = sessiId(cl_id)
	#todo cosa invio a ps/dataflow? per ora data
	data = {"event_name":"session_begin", "session_id": session_id}
	toPubSub(data)

	toClient = {"session_id" : session_id, "format" : "JSONorMP"}
	b=json.dumps(toClient)
	return b

#funzione per calcolare l'id della sessione
def sessiId(client_id):
	currentTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	session = client_id+currentTime
	session_id = (hash(session))
	return session_id


@app.route('/EventFrame', methods=['POST'])
#server-istances group
def event():
	#qui mi arriva o una lista di eventi o un evento e mando dirett a pubsub
	event_data = request.data
	data = json.loads(event_data)
	a = {"TDB" : "TDB"}
	b=json.dumps(a)
	return b

@app.route('/sessionEnd', methods=['POST'])
#server-istances group
def end():
	event_data = request.data
	data = json.loads(event_data)
	global SESSIONS
	for item in SESSIONS:
		if(item["session_id"] == data["session_id"]):
			SESSIONS.remove(item)
	a = {"TDB" : "TDB"}
	b=json.dumps(a)
	return b


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
