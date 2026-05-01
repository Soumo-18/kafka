import { kafkaClient } from "./kafka-client.js";

async function setup () {
    const admin = kafkaClient.admin()
    console.log(`Kafka Admin Connecting..`)
    await admin.connect()
     console.log(`Kafka Admin Connected Successfully`)

     await admin.createTopics( {
        topics:[{ topic:'location-updates', numPartitions:2 }] //max 2 consumers

     })

     await admin.disconnect()

}

setup()