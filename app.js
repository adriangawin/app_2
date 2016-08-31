var AWS = require("aws-sdk"),
    fs = require('fs'),
    helpers = require("./helpers");
AWS.config.loadFromPath('./config.json');
var s3 = new AWS.S3(),
    APP_CONFIG_FILE = "./app.json",
    tablicaKolejki = helpers.readJSONFile(APP_CONFIG_FILE),
    linkKolejki = tablicaKolejki.QueueUrl,
    sqs=new AWS.SQS(),
    simpledb = new AWS.SimpleDB();
//GraphicsMagic
var gm = require('gm').subClass({imageMagick: true});

//funkcja - petla wykonuje sie caly czas
var myServer = function(){
	
	//parametr do funkcji pobierającej wiadomość z kolejki
	var params = {
		QueueUrl: linkKolejki,
		AttributeNames: ['All'],
		MaxNumberOfMessages: 1,
		MessageAttributeNames: ['key','bucket'],
		VisibilityTimeout: 10,//na tyle sec nie widac jej w kolejce
		WaitTimeSeconds: 0//to na 0 
	};
	
	//odbiera wiadomość
	sqs.receiveMessage(params, function(err, data) {
	if (err) {
		console.log(err, err.stack); // an error occurred
	}
	else {
		//console.log(JSON.stringify(data, null, 4));
		
		//Czy jest jakaś wiadomość
		if(!data.Messages) {
			console.log("Brak wiadomości w kolejce!");
		} else {
			
			//pobranie danych z body wiadomosci w kolejce i zrobienie z nich tablicy
			//handler do ussunięcia wiadomości z kolejki
			var ReceiptHandle_forDelete = data.Messages[0].ReceiptHandle;
			//console.log(data.Messages[0].Body);
			var messageinfo = data.Messages[0].Body.split('/');
			console.log("Otrzymano wiadomość: bucket - "+messageinfo[0]+", key - "+messageinfo[1]);
			//parametry do pobrania pliku (obiektu)
			var params2 = {
				Bucket: 'lab4-weeia',
				//Prefix: 'pawel.jablonski/',
				Key: messageinfo[0]+'/'+messageinfo[1],
				//Region: "us-west-2"
				
			};
			//zapisujemy plik z s3 na dysku
			var file = require('fs').createWriteStream('tmp/'+messageinfo[1]);
			var requestt = s3.getObject(params2).createReadStream().pipe(file);
                        //console.log(requestt);
			//po zapisie na dysk
			requestt.on('finish', function (){
				console.log('Zapisano plik: ' + messageinfo[1] + ' ,na dysk');

				gm('tmp/'+messageinfo[1]).colors(40)
				.write('tmp/'+messageinfo[1], function (err) {
				if (err) {
					console.log(err);
				}
				//po udanej zmienie w pliku
				else {
					console.log('Plik przetworzono z powodzeniem');	
					
					//wrzucamy na s3 nowy plik
					var fileStream = require('fs').createReadStream('tmp/'+messageinfo[1]);
					fileStream.on('open', function () {
						var paramsu = {
							Bucket: 'lab4-weeia',
							//Prefix: 'pawel.jablonski',
							Key: 'processed/'+messageinfo[1],
							ACL: 'public-read',
							Body: fileStream,
						};
						s3.putObject(paramsu, function(err, datau) {
						if (err) {
							console.log(err, err.stack);
						}
						else {   
							//console.log(datau);
							console.log('Upload pliku');
							
							
							//zmieniamy info w bazie że już przerobiony plik
							var d = new Date();
							var paramsdb = {
								Attributes: [
									{ 
									Name: data.Messages[0].Body, 
									Value: "yes", 
									Replace: true
									}
								],
								DomainName: "PawelKrzysiek", 
								ItemName: 'ITEM001'//+d.getTime()
							};
							simpledb.putAttributes(paramsdb, function(err, datass) {
							if (err) {
								console.log('Blad zapisu do bazy'+err, err.stack);
							}
							else {
								console.log("Zapisano do bazy");
								//usuwanie wiadomosci z kolejki
								var params = {
								  QueueUrl: linkKolejki,
								  ReceiptHandle: ReceiptHandle_forDelete
								};
								sqs.deleteMessage(params, function(err, data) {
								  if (err) console.log(err, err.stack); // an error occurred
								  else     console.log("Usunieto wiadomosc z kolejki: " + linkKolejki.substring(49));           // successful response
								});
							}
							});
						}
						}
					);
					});	
				}
				});	
			});
		}
	}
	});
	setTimeout(myServer, 10000);
}			

	

	
		
	
//odpalamy petle
myServer();
