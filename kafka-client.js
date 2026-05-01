import { Kafka } from "kafkajs";

export const kafkaClient = new Kafka( {
    client:'chaicode',
    brokers:['localhost:9092']
})