from flask import Flask, jsonify

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
