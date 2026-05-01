import http from 'node:http'
import path from 'node:path'

import express from 'express'
import { Server } from 'socket.io'

import { kafkaClient } from './kafka-client.js'

async function main() {
    const PORT = process.env.PORT ?? 8000

    const app = express()
    const server = http.createServer(app)
    const io = new Server (server)

    const kafkaProducer = kafkaClient.producer()
    await kafkaProducer.connect()

    const kafkaConsumer = kafkaClient.consumer({ groupId:`scoket-server-${PORT}`})
    await kafkaConsumer.connect()

    await kafkaConsumer.subscribe({ 
        topics:['location-updates'],
         fromBeginning:true 
    })
    kafkaConsumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat}) => {   //whenever we get a msg this callback func will run
            const data = JSON.parse(message.value.toString())
            console.log(`Kafka Consumer Data Received`, { data })
            io.emit('server:location-update', {
                id: data.id,
                 lat:data.lat,
                  lng:data.lng 
            } )
            await heartbeat()
        },
    })

    io.attach(server)


    io.on('connection', (socket) => {
        console.log(`[Socket:${socket.id}]: Connected Successfully`)

        socket.on('client:location:update', async (locationData) => {
            const { lat, lng } = locationData
            console.log(
                `[Socket:${socket.id}]:client:location:update: `,
                 locationData
            )

            await kafkaProducer.send({ topic:'location-updates', 
                messages:[
                    {
                        key:socket.id,
                        value:JSON.stringify({id: socket.id, lat, lng })
                    }
                ]
            })
        })
        socket.on('disconnect', () => {
            console.log(`[Socket:${socket.id}]: Disconnected`);
            io.emit('server:user-disconnected', socket.id);
        })
    })


    app.use(express.static(path.resolve('./public')))

    app.get('/health', (req,res) => {
        return res.json({ healthy:true})
    })

    server.listen(PORT, () => console.log(`Server Running on http://localhost:${PORT}`))
}


main()