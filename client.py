from flask import Flask, request, Response, jsonify, json

app = Flask(__name__)

@app.route('/')
def index():
	return """
<form action="/start" method="POST" enctype="multipart/form-data">
    <button name="vedi registro" type="submit">Session Begin</button>
</form>

<form action="/application_start" method="POST" enctype="multipart/form-data">
    <button name="vedi registro" type="submit">Send APPLICATION_START</button>
</form>

<form action="/screen_loaded" method="POST" enctype="multipart/form-data">
    <button name="vedi registro" type="submit">Send SCREEN_LOADED</button>
</form>


<form action="/application_exit" method="POST" enctype="multipart/form-data">
    <button name="vedi registro" type="submit">Send APPLICATION_EXIT</button>
</form>

<form action="/end" method="POST" enctype="multipart/form-data">
    <button name="vedi registro" type="submit">Session end</button>
</form>
"""



@app.route('/start', methods=['POST'])
#client-decoder
def startSession():
	r = requests.post('http://10.1.2.99:80/Start', data=b)
	risposta = r.content
	return risposta



if __name__ == '__main__':
	# This is used when running locally. Gunicorn is used to run the
	# application on Google App Engine. See entrypoint in app.yaml.
	app.run(host='0.0.0.0', port=8080, debug=True)
# [END gae_flex_storage_app]
