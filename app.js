/* LINKS IMPORTANTES 

	KAFKA
	- http://stackoverflow.com/questions/37931408/how-to-read-data-from-topic-using-kafka-node
	- https://www.npmjs.com/package/kafka-node

	MONGODB
	- http://treehouse.github.io/installation-guides/mac/mongo-mac.html
	- https://blog.xervo.io/mongodb-tutorial?ref=driverlayer.com/web
	- https://github.com/mongodb/js-bson

*/

// Importa os modulos externos
var BSON = require('bson')
var bson = new BSON();
var http = require('http');
var kafka = require('kafka-node');
var mongodb = require('mongodb');

// Cria o client do MongoDB e sua respectiva url
var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/humberto';

// Cria o consumer do Kafka com o endereço e porta
var Consumer = kafka.Consumer,
    client = new kafka.Client("localhost:2181"),
    
    // Configura o tópico que ele irá consumir 
    consumer = new Consumer(
        client, [ { topic: 'test', partition: 0 } ], { autoCommit: false }
    );

// Evento para quando acessar a url do localhost
function onRequest(request, response){

	console.log("USUARIO FEZ UM REQUEST EM: " + request.url); // URL que foi acessada

 	// Resposta do cliente
	response.writeHead(200, {"Context-Type" : "text/plain"} );
	response.write("printando dados"); 
	response.end(); 

}

// Evento do consumer do Kafka
consumer.on('message', function (message) {
    
    // Verfica se a mensagem do kafka é um json
    if (isJSON(message.value)) {

    	// Converte para JSON
    	var newMessage = JSON.parse(message.value);
    	var data = bson.serialize(newMessage)

	    // Conecta com o MongoDB
		MongoClient.connect(url, function (err, db) {
			if (err) {
				console.log('Erro ao conectar no MongoDB: ', err);
			} else {

				// Se conectar 
				console.log('Conexão estabilizada na URL: ', url);

				// Escolhe o schema em que será inserido
				var collection = db.collection('alunos');

				// Insere o aluno no schema indicado
				collection.insert(newMessage, function (err, result) {
					if (err) {
						console.log(err);
					} else {
						console.log('Inseriu %d documentos dentro do schema "alunos". Os documentos inseridos com o "_id" são:', result.length, result);
					}
					
					// Fecha conexão
				  	db.close();
				});

				db.close();
			}
		});
		// exemplo de json: {"name": "lucas silva e silva", "id": 19, "idade": 13}
	}

	// Imprime a mensagem enviada
    console.log(message.value);
});


/* Criação do servidor */
http.createServer(onRequest).listen(8888);
console.log("Server is running ...");

// Função que verifica se a string é um json
function isJSON(str) {
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}