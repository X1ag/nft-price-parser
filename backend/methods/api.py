import json
from quart import Quart, jsonify
from quart_cors import cors

app = Quart(__name__)
app = cors(app, allow_origin='http://92.205.129.142:8080, http://localhost:4200')

# Пример маршрута для GET-запроса
@app.route('/dyweapi/v1/getData/<address>', methods=['GET'])
async def get_data(address):
   try:
       with open(f'candles/candles{address}.json', 'r+') as f:
            data = json.load(f)
            return jsonify(data)
   except FileNotFoundError:
       return jsonify({"error": "File not found"}), 404
   except json.decoder.JSONDecodeError:
       return jsonify({"error": "invalid Json"}), 404
   

@app.route('/dyweapi/v1/getHistory/<address>', methods=['GET'])
async def get_history(address):
   try:
       with open(f'candles/candleHistory{address}.json', 'r') as f:
            data = json.load(f)
            return jsonify(data)
   except FileNotFoundError:
       return jsonify({"error": "File not found"}), 404
   except json.decoder.JSONDecodeError:
       return jsonify({"error": "invalid Json"}), 404
     
@app.route('/health', methods=['GET'])
async def health():
    return "Health check: OK \n"

def main():
    app.run(debug=True)
