import { Kafka } from "kafkajs";

// export const kafkaClient = new Kafka( {
//     clientId:'chaicode',
//     brokers:['localhost:9092']
// })
const brokers = [process.env.KAFKA_BROKER || 'localhost:9092'];

const saslConfig = process.env.KAFKA_USERNAME ? {
    ssl: true,
    sasl: {
        mechanism: 'scram-sha-256', 
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
    }
} : {}; 

export const kafkaClient = new Kafka({
    clientId: 'live-location-tracker',
    brokers: brokers,
    ...saslConfig
});