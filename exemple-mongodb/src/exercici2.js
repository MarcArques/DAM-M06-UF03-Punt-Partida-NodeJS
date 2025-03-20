const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const PDFDocument = require('pdfkit');
const winston = require('winston');
require('dotenv').config();

//  Configurar logger
const logDir = path.join(__dirname, '../../data/logs');
if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.json(),
    transports: [
        new winston.transports.File({ filename: path.join(logDir, 'exercici2.log') }),
        new winston.transports.Console({ format: winston.format.simple() })
    ]
});

//  Configuración MongoDB
const uri = 'mongodb://root:password@localhost:27017/';
const client = new MongoClient(uri);
const dbName = 'stackexchange_db';
const collectionName = 'questions';

//  Directorio de salida para los PDFs
const outputDir = path.join(__dirname, '../../data/out');
if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

//  Consulta 1: Preguntas con `ViewCount` superior a la media
async function getAboveAverageViewCountQuestions(db) {
    const collection = db.collection(collectionName);

    // Calcular la media de ViewCount
    const avgResult = await collection.aggregate([
        { $group: { _id: null, avgViewCount: { $avg: "$question.ViewCount" } } }
    ]).toArray();

    const avgViewCount = avgResult[0]?.avgViewCount || 0;
    logger.info(`Media de ViewCount: ${avgViewCount}`);

    // Obtener preguntas con `ViewCount` superior a la media
    return await collection.find({ "question.ViewCount": { $gt: avgViewCount } }).toArray();
}

//  Consulta 2: Preguntas con ciertas palabras clave en el `Title`
async function getQuestionsWithKeywords(db) {
    const collection = db.collection(collectionName);
    const keywords = ["pug", "wig", "yak", "nap", "jig", "mug", "zap", "gag", "oaf", "elf"];

    // Expresión regular para buscar palabras clave en el título
    const regex = new RegExp(keywords.join("|"), "i");

    return await collection.find({ "question.Title": { $regex: regex } }).toArray();
}

//  Función para generar PDFs
function generatePDF(fileName, title, questions) {
    const filePath = path.join(outputDir, fileName);
    const doc = new PDFDocument();

    doc.pipe(fs.createWriteStream(filePath));

    doc.fontSize(16).text(title, { align: "center" }).moveDown(1);

    questions.forEach((q, index) => {
        doc.fontSize(12).text(`${index + 1}. ${q.question.Title}`);
    });

    doc.end();
    logger.info(` Archivo PDF generado: ${filePath}`);
}

//  Función principal
async function main() {
    try {
        await client.connect();
        logger.info(' Conectado a MongoDB');

        const db = client.db(dbName);

        // Ejecutar consultas
        logger.info(' Ejecutando consulta 1...');
        const aboveAvgQuestions = await getAboveAverageViewCountQuestions(db);
        logger.info(` Preguntas encontradas (ViewCount > Media): ${aboveAvgQuestions.length}`);

        logger.info(' Ejecutando consulta 2...');
        const keywordQuestions = await getQuestionsWithKeywords(db);
        logger.info(` Preguntas encontradas (Keywords en Title): ${keywordQuestions.length}`);

        // Generar PDFs
        generatePDF("informe1.pdf", "Preguntas con ViewCount superior a la media", aboveAvgQuestions);
        generatePDF("informe2.pdf", "Preguntas con palabras clave en el título", keywordQuestions);

    } catch (error) {
        logger.error('Error en la ejecución:', error);
    } finally {
        await client.close();
        logger.info('Conexión cerrada');
    }
}

main();
