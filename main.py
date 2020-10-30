
from flask import Flask, request, Response, jsonify, json
import datetime;

app = Flask(__name__)


@app.route('/')
def index():
	return """
<form method="POST" action="/addResource" enctype="multipart/form-data"
  <label for="fname">IL SERVIZIO SUL GRUPPO DI ISTANZE SUPERVISIONATO E' ONLINE</label><br>
  <br>
</form>
"""

@app.route('/Start', methods=['POST'])
#server-istances group
def event():
	return "funziona"



if __name__ == '__main__':
	# This is used when running locally. Gunicorn is used to run the
	# application on Google App Engine. See entrypoint in app.yaml.
	app.run(host='0.0.0.0', port=80, debug=True)
# [END gae_flex_storage_app]
