from flask import Flask, request, Response, jsonify


app = Flask(__name__)

@app.route('/')
def index():
	return """
<form method="POST" action="/addResource" enctype="multipart/form-data"
  <label for="fname">Nome  risorsa da scaricare:</label><br>
  <input type="text" id="nomeintero" name="nomeintero" value="nomeItem_nomeRisorsa">
  <input type="submit">
  <br>
</form>
"""

@app.route('/start-event', methods=['POST'])
#server-istances group
def events():
	event_data = request.data.decode('utf-8')
	a = "200"
	#r = requests.post('http://127.0.0.1:8080/ack', data=a)
	b=jsonify(a)
	return b


'''
app = Flask(__name__)


@app.route('/', methods=['GET'])
def say_hello():
    return "HO ATTIVATO PUB/SUB"

@app.route('/start-event', methods=['POST'])
#server-istances group
def events():
	a = "ok fra"
	b=jsonify(a)
	return b


@app.route('/a', methods=['GET'])
def saay_hello():
    return "HO ATTIVATO PUB/SUB MA SOLO SE TO CRIR"

@app.route('/c', methods=['GET'])
def saya_hello():
    return "HO ATTIVATO PUB/SUB MA SOLO SE TO CRIR pero stavot ha prmut c"
'''
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
