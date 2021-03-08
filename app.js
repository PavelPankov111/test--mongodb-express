const express = require('express');
const url = 'mongodb://127.0.0.1:27017/?compressors=zlib&gssapiServiceName=mongodb';
var MongoClient = require('mongodb').MongoClient;
const { PORT = 3000 } = process.env;
const app = express();

var ObjectId = require('mongodb').ObjectID;

MongoClient.connect(url, async function(err, database) {
    if (err) throw err;
    const db = database.db('test-express')
    const collectionFirst = db.collection('fisrst')
    const collectionResult = db.collection('result')
    const collectionSecond = db.collection('second')
    const allLocation = await collectionFirst.aggregate([{ $project: { location: 1, _id: 0, country: 1 } }]).toArray()
        // get all countryes
    const bedfordshire = 'Bedfordshire'
    const borders = 'Borders'
    const buckinghamshire = 'Buckinghamshire'
    const berkshire = 'Berkshire'
    const avon = 'Avon'
    const cambridgeshire = 'Cambridgeshire'
    const bedfordshireSudents = await collectionFirst.aggregate([{ $match: { country: bedfordshire } }]).toArray()
    const bordersSudents = await collectionFirst.aggregate([{ $match: { country: borders } }]).toArray()
    const buckinghamshireSudents = await collectionFirst.aggregate([{ $match: { country: buckinghamshire } }]).toArray()
    const berkshireSudents = await collectionFirst.aggregate([{ $match: { country: berkshire } }]).toArray()
    const avonSudents = await collectionFirst.aggregate([{ $match: { country: avon } }]).toArray()
    const cambridgeshireSudents = await collectionFirst.aggregate([{ $match: { country: cambridgeshire } }]).toArray()

    // Массив всех студентов для добавлния в третью коллекцию 
    let allStudents = []

    // Получаем из 1 коллекции нужные данные для добавления полей longitude и latitude
    allLocation.forEach(function(i) {
        const location = i.location.ll
        const country = i.country
        let obj = {}
        obj.longitude = location[0]
        obj.latitude = location[1]
        obj.id = country
            // Прокидываем результат в гланый массив со всеми студентами
        allStudents.push(obj)
    })

    // Получаем поля allDocs и count для студентов каждой страны
    async function getSumAndCountStudents(array, country) {
        // На вход функция принимает массив студентов из определенной страны и саму страну ( всего 6 стран )

        // Добавляем количесство студентов в массив numberStudents
        let numberStudents = []
        array.forEach(i => numberStudents.push(i.students.length))

        // Складываем все это количесство студетов
        const sumStudents = numberStudents.map(i => x += i, x = 0).reverse()[0]
            // Получаем доступ к второй коллекции для вычисления разницы 
        let secondSudents = await collectionSecond.aggregate([{ $match: { country: country } }]).toArray()
        secondSudents.forEach(item => {
            // Получаем разницу студентов
            const difference = item.overallStudents - sumStudents

            // Фильтруем массив и получаем нужную нам страну
            const filterCountry = allStudents.filter(el => el.id === country)

            // Добавляем в массив поля count и allDocs
            filterCountry.forEach((i) => {
                i.count = filterCountry.length
                i.allDocs = difference
            })
        })

        return allStudents
            // функция возвращает обновленный массив allStudents
    }

    getSumAndCountStudents(bedfordshireSudents, bedfordshire)
    getSumAndCountStudents(bordersSudents, borders)
    getSumAndCountStudents(buckinghamshireSudents, buckinghamshire)
    getSumAndCountStudents(berkshireSudents, berkshire)
    getSumAndCountStudents(avonSudents, avon)
    getSumAndCountStudents(cambridgeshireSudents, cambridgeshire)
        .then(response => {
            // Парсим ответ только у последнего вызовв функции, потому что он возвращет результат и всех предыдущих вызовов
            let obj = { _id: -1 };

            // Превращем массив в обьект для отправки в коллекцию 
            response.forEach((el) => {
                Object.assign(obj, el);
                obj._id = ObjectId()
                collectionResult.insertOne(obj, function(err, result) {
                    if (err) throw err
                    if (result) console.log(result.ops)
                    database.close()
                })
            });

        })
        .catch(err => console.log(err))

    app.listen(PORT)
});

app.all('*', () => { throw new Error('Запрашиваемый ресурс не найден'); });