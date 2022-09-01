const MongoClient = require("mongodb").MongoClient;
    
const url = "mongodb://localhost:27017/";
const mongoClient = new MongoClient(url);
const fs = require('fs');
const firstFile = fs.readFileSync('./data/first.json');  
const secondFile = fs.readFileSync('./data/second.json');  

async function run() {
    try {
        await mongoClient.connect();
        const db = mongoClient.db("test");
        
        const collectionFirst = db.collection("first");
        const collectionSecond = db.collection("second");

        await collectionFirst.insertMany(JSON.parse(firstFile));
        await collectionSecond.insertMany(JSON.parse(secondFile));

        const pipeline = [
            { 
                $group: { 
                    _id: "$country", 
                    latitude: { 
                        $push: { 
                            $first: "$location.ll" 
                        } 
                    }, 
                    longitude: { 
                        $push: { 
                            $last: "$location.ll" 
                        } 
                    },
                    count: { 
                        $sum: 1 
                    }
                }
            },
            {
                $lookup: {
                    from: "second",
                    localField: "_id",
                    foreignField: "country",
                    as: "allDiffs"
                }
            },
            {
                $set: {
                    allDiffs: { 
                        $subtract: [{ $first: "$allDiffs.overallStudents"}, "$count"]
                    }
                }
            },
            { 
                $merge: { 
                    into: "third" 
                } 
            }
        ]
        const aggCursor = collectionFirst.aggregate(pipeline);
        for await (const doc of aggCursor) {
            console.log(doc);
        }
        console.log("See 'third' collection in MongoDB.")
    }
    catch(err) {
        console.log(err);
    } 
    finally {
        await mongoClient.close();
    }
}
run();