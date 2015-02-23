#! /usr/bin/env node

"use strict";

var MicroLector = require("../tools/node");
var argv = require('minimist')(process.argv.slice(2), {
  alias: {h: 'help', v: 'version'}
});
var fs = require('fs');
var MongoClient = require('mongodb').MongoClient;
var pc = require('path-complete');
var includes = require('lodash.includes');

if (argv.help || argv._.length === 0) {
  var cmd = require('path').basename(process.argv[1]);
  console.log("Uso:\n  "+cmd+" [opciones] <tipo de fichero> <base de datos>\n\nTipos:\n  epf     Encuesta de Presupuestos Familiares\n\nOpciones:\n  -h, --help     imprimir esta ayuda");
  process.exit();
}

var fileType = argv._[0];
var dbName = argv._[1] || fileType;

fs.readdir(__dirname+'/../formats/', function(err, files) {
  if (err) error('Error al leer definiciones de ficheros: '+err.message);
  if (!includes(files, fileType+'.json')) error('No se encontró definición del fichero: '+fileType);
  MongoClient.connect('mongodb://localhost:27017/'+dbName, function(err, db) {
    if (err) error('Error al conectar con MongoDB '+dbName+': '+err.message);
    processDefinition(require('../formats/'+fileType+'.json'), db, function(err, result) {
      if (err) error('Error al procesar fichero: '+err.message);
      console.log('Importación finalizada');
      process.exit();
    });
  });
});

function processDefinition(definition, db, callback) {
  var postponed = [];
  if (definition.length === undefined) {
    definition = [definition];
  }
  next();
  function next() {
    if (definition.length) {
      processFile(definition[0]);
    } else {
      callback();
    }
  }
  function processFile(file) {
    var records = 0;
    var spinner = ['|', '/', '—', '\\'];
    if (file['parent']) {
      db.collectionNames(function(err, collectionNames) {
        if (includes(collectionNames, file['parent']['file'])) {
          if (includes(postponed, file['file'])) return callback(new Error('Definición incompleta de los ficheros'));
          postponed.push(file['file']);
          definition.push(definition.shift());
          return next();
        }
      });
    }
    process.stdout.write("Ruta al fichero de datos ("+file['file']+"): ");
    pc.getPathFromStdin(function(path) {
      var remainder = '';
      fs.createReadStream(path, {encoding: 'utf8'}).on('error', function(err) {
        callback(err);
      }).on('data', function(chunk) {
        var lines = chunk.split(/\r\n|\r|\n/);
        lines.forEach(function(line, index) {
          if (index === 0) {
            line = remainder+line;
          } else if (index === lines.length-1) {
            remainder = line;
            return;
          }
          if (line.length === 0) {
            return;
          } else if (line.length !== file['length']) {
            return callback(new Error('El fichero no coincide con la definición'));
          }
          processLine(line, file['fields'], function(err, document) {
            if (err) return callback(err);
            if (file['parent']) {
              var id = '';
              file['parent']['key'].forEach(function(key) {
                id += document[key];
                delete document[key];
              });
              var obj = {};
              obj[file['file']] = document;
              db.collection(file['parent']['file']).update({_id: id}, {$push: obj}, function(err, result) {
                if (err) return callback(err);
              });
            } else {
              document['_id'] = '';
              file['key'].forEach(function(key) {
                document['_id'] += document[key];
              });
              db.collection(file['file']).insert(document, function(err, result) {
                if (err) return callback(err);
              });
            }
            records++;
            process.stdout.cursorTo(0);
            process.stdout.write(spinner[0]);
            spinner.push(spinner.shift());
          });
        });
      }).on('end', function() {
        process.stdout.cursorTo(0);
        console.log(records+' registros insertados');
        definition.shift();
        next();
      });
    });
  }
}

function processLine(line, fields, callback) {
  var document = {};
  var processedFields = 0;
  var charPos = 0;
  fields.forEach(function(field) {
    processValue(field, line.substr(charPos, field['length']), function(err, value){
      if (err) return callback(err);
      if (value !== null) {
        document[field['name']] = value;
      }
      processedFields++;
      if (processedFields === fields.length) {
        callback(null, document);
      }
    });
    charPos += field['length'];
  });
}

function processValue(field, value, callback) {
  switch (field['type']) {
    case 'integer':
      value = parseInt(value);
      if (isNaN(value) || (field['null'] && value === field['null'])) {
        value = null;
      }
      break;
    case 'double':
      value = parseInt(value);
      if (isNaN(value) || (field['null'] && value == field['null'])) {
        value = null;
      } else {
        value = value/Math.pow(10, field['decimals']);
      }
      break;
    case 'key':
      if (!field['keytype']) {
        return callback(new Error('Error en la definición del campo '+field['name']));
      }
      switch (field['keytype']) {
        case 'integer':
          var key = parseInt(value);
          if (isNaN(key) || (field['null'] && key === field['null'])) {
            value = null;
          } else {
            value = field['values'][key];
          }
          break;
        case 'string':
          if (value.trim() === '' || (field['null'] && value === field['null'])) {
            value = null;
          } else {
            value = field['values'][value];
          }
          break;
        case 'tree':
          var charPos = 0;
          var value = [value];
          var tree = field['values'];
          field['keylength'].forEach(function(len) {
            var key = value[0].substr(charPos, len);
            if (typeof tree[key] === 'string') {
              value.push(tree[key]);
            } else {
              value.push(tree[key]['name']);
              tree = tree[key]['values'];
              charPos += len;
            }
          });
          break;
        default:
          return callback(new Error('Error al procesar valor de tipo '+field['type']));
          break;
      }
      break;
    default:
      return callback(new Error('Error al procesar valor de tipo '+field['type']));
      break;
  }
  callback(null, value);
}

function error(message) {
  console.log(message);
  process.exit(1);
}
