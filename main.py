from flask import Flask
app = Flask(__name__)


@app.route('/', methods=['GET'])
def say_hello():
    return "HO ATTIVATO PUB/SUB"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
