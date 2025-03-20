const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const xml2js = require('xml2js');
const { decode } = require('html-entities');
const winston = require('winston');
require('dotenv').config();

//   Configurar el logger
const logDir = path.join(__dirname, '../../data/logs');
if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.json(),
    transports: [
        new winston.transports.File({ filename: path.join(logDir, 'exercici1.log') }),
        new winston.transports.Console({ format: winston.format.simple() })
    ]
});

//   Configuración de MongoDB
const uri = 'mongodb://root:password@localhost:27017/';
const client = new MongoClient(uri);
const dbName = 'stackexchange_db';
const collectionName = 'questions';

//   Ruta del archivo XML (ajústalo si es necesario)
const xmlFilePath = path.join(__dirname, '../../data/Posts.xml');

//   Función para leer y parsear el XML
async function parseXML(filePath) {
    try {
        const xmlData = fs.readFileSync(filePath, 'utf-8');
        const parser = new xml2js.Parser({ explicitArray: false, mergeAttrs: true });

        return new Promise((resolve, reject) => {
            parser.parseString(xmlData, (err, result) => {
                if (err) reject(err);
                else resolve(result);
            });
        });
    } catch (error) {
        logger.error(' Error leyendo el archivo XML:', error);
        throw error;
    }
}

//   Función para procesar las preguntas del XML
function processQuestions(data) {
    let questions = data.posts.row;

    // Asegurar que sea un array
    if (!Array.isArray(questions)) questions = [questions];

    // Filtrar solo preguntas (PostTypeId = "1")
    const filteredQuestions = questions
        .filter(q => q.PostTypeId === '1' && q.ViewCount)
        .map(q => ({
            question: {
                Id: q.Id,
                PostTypeId: q.PostTypeId,
                AcceptedAnswerId: q.AcceptedAnswerId || null,
                CreationDate: q.CreationDate,
                Score: parseInt(q.Score),
                ViewCount: parseInt(q.ViewCount),
                Body: decode(q.Body || ''),
                OwnerUserId: q.OwnerUserId || null,
                LastActivityDate: q.LastActivityDate,
                Title: q.Title || 'Sin título',
                Tags: q.Tags ? q.Tags.replace(/<|>/g, '').split(' ') : [],
                AnswerCount: parseInt(q.AnswerCount || '0'),
                CommentCount: parseInt(q.CommentCount || '0'),
                ContentLicense: q.ContentLicense || 'Desconocida'
            }
        }))
        .sort((a, b) => b.question.ViewCount - a.question.ViewCount) 
        .slice(0, 10000); 

    return filteredQuestions;
}

//   Insertar datos en MongoDB
async function insertData(questions) {
    try {
        await client.connect();
        logger.info('  Conectado a MongoDB');

        const database = client.db(dbName);
        const collection = database.collection(collectionName);

        // Eliminar datos existentes
        await collection.deleteMany({});
        logger.info('⚠️ Datos antiguos eliminados');

        // Insertar los nuevos datos
        const result = await collection.insertMany(questions);
        logger.info(`  ${result.insertedCount} documentos insertados correctamente`);

    } catch (error) {
        logger.error(' Error insertando datos en MongoDB:', error);
    } finally {
        await client.close();
        logger.info(' Conexión cerrada');
    }
}

//   Ejecutar todo el proceso
async function main() {
    logger.info(' Iniciando proceso de carga de datos');
    
    try {
        const xmlData = await parseXML(xmlFilePath);
        logger.info('Archivo XML procesado correctamente');

        const questions = processQuestions(xmlData);
        logger.info(`Se han procesado ${questions.length} preguntas`);

        await insertData(questions);
        logger.info('  Proceso finalizado correctamente');
    } catch (error) {
        logger.error(' Error en la ejecución principal:', error);
    }
}

main();
