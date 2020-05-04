const https = require('https');
const stream = require('stream');
const express = require('express');
const {Storage} = require('@google-cloud/storage');

// ---------------- DATA

var app = express();

const storage = new Storage({
    keyFilename: "incendis-418fb8fd3b9b.json",
    projectId: "incendis"
});

const bucket = storage.bucket('incendis');

const json_file = "xdde.json";
const csv_file = "xdde.csv";

const gc_json_file = bucket.file('widgets/Meteo/'+json_file);
const gc_csv_file = bucket.file('widgets/Meteo/'+csv_file);

var json = null;

const apiKey = "";

var last_hour = new Date();
last_hour.setHours(last_hour.getHours()-1);

var path = "/xdde/v1/catalunya/" + timeToString(last_hour, "/");

var options = {
    host: 'api.meteo.cat',
    path: path,
    headers : {
      "x-api-key": apiKey
    }
};

// --------------- FUNCTIONS

// Funció per convertir temps a format yyyy/mm/dd/hh
function timeToString(time, char){
    var hh = String(time.getHours()).padStart(2, '0');
    var dd = String(time.getDate()).padStart(2, '0');
    var mm = String(time.getMonth() + 1).padStart(2, '0');
    var yyyy = String(time.getFullYear());

    return yyyy + char + mm + char + dd + char + hh;
};

// Funció per consultar les dades de les XDDE
var callback = function (response) {

    var bodyChunks = [];

    response.on('data', function (chunk) {
        bodyChunks.push(chunk);

    }).on('end', function () {
        var body = Buffer.concat(bodyChunks);
        var content = JSON.parse(body);

        if (response.statusCode == '200') {
            var data = {};
            data['date'] = timeToString(last_hour,"-");
            data['coord'] = [];

            content.forEach(function (r) {
                if (r.nuvolTerra) {
                    var coord = {};
                    coord['latitude'] = r.coordenades.latitud;
                    coord['longitude'] = r.coordenades.longitud;
                    data['coord'].push(coord);
                }
            });
            saveData(data);
        } else {
            console.log("Status Code: " + response.statusCode);
        }
    })
};

// Funció per guardar les dades noves i actualitzar els fitxers
function saveData(new_data){

    // Afegim noves dades
    json.push(new_data);

    // Guardem al fitxer JSON
    var body = JSON.stringify(json);
    uploadFile(gc_json_file, body);

    // Guardem el fitxer CSV
    var csv = JSONtoCSV(json);
    uploadFile(gc_csv_file, csv);
}

// Funció per actualitzar els fitxers de Google Cloud Storage
async function uploadFile(gc_file, content){
    const dataStream = new stream.PassThrough();
    dataStream.push(content);
    dataStream.push(null);
    dataStream.pipe(gc_file.createWriteStream());
    console.log("Updated file " + gc_file.name);
}

// Funció per convertir les dades de JSON a CSV
function JSONtoCSV(json){
    var csv = "latitude;longitude\n";
    json.forEach(function(hour){
        hour['coord'].forEach(function(element){
            csv = csv + element.latitude + ";" + element.longitude + "\n"; 
        })
    });
    return csv;
}

// Funció per actualitzar dades

function updateFiles(){
    // Descarrega el fitxer JSON
    bucket.file('widgets/Meteo/'+json_file).download(function(err, contents){
        if(err){
            console.log("Error downloading file");
        } else {
        // Obtenim el contingut
        json = JSON.parse(contents);
    
        // Eliminem dades antigues
        while (json.length > 23) {
            json.shift();
        }
        // Realitzem petició API i guardem les dades noves
        var request = https.get(options, callback);
        request.on('error', function (e) {
            console.log('ERROR: ' + e.message);
        });
    }
    });
}

// --------------- MAIN

app.get('/', function(req,res){
    updateFiles();
    res.status(200).send("Actualitzant dades...").end();
});

app.listen(8080, function () {
    console.log("Escoltant peticions");
});



